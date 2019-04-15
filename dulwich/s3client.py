from binascii import hexlify, unhexlify
import os
import tempfile
import time
import zlib
import boto3

import threading
from queue import Queue

# for the object store
from dulwich.object_store import PackBasedObjectStore, ShaFile, ObjectStoreIterator
from dulwich.objects import Blob
from dulwich.pack import PackData, iter_sha1, write_pack_index_v2, Pack, load_pack_index_file
from io import StringIO

# for the refstore
from dulwich.repo import RefsContainer, SYMREF

# for the repo
from dulwich.repo import BaseRepo

import logging
log = logging.getLogger('git-remote-s3')

"""Support for dulwich (git) storage structures on Amazon S3.

This module allows replicating the structure of a git repository on an S3 bucket. This
approach is much lower in overhead then a full fledged file-system, as the core structure
of git, objects, can be translated 1:1 to S3 keys.

The names of the resulting repository is laid in such a way that, if copied over onto
an empty git repository, the result is a valid git repository again.

It is recommend to use this on a non-versioned bucket. A good degree of concurreny can be
achieved with almost no effort: Since uploaded objects are named after their hash, an
object file will always have the same contents if it has its name. Upload the same object
multiple times by accident is therefore not an issue.

When manipulating refs however, you will most likely need to implement a locking mechanism.
"""


class S3PrefixFS(object):
	_prefix = ''

	@property
	def prefix(self):
		return self._prefix

	@prefix.setter
	def prefix(self, value):
		# strip leading and trailing slashes, remote whitespace
		self._prefix = value.strip().rstrip('/').lstrip('/').strip()
		# normalize to one trailing '/'
		if self._prefix: self._prefix += '/'


class S3RefsContainer(RefsContainer, S3PrefixFS):
	"""Stores refs in an amazon S3 container.

	Refs are stored in S3 keys the same way as they would on the filesystem, i.e. as
	contents of paths like refs/branches/...

	It is up to to the user of the container to regulate access, as there is no locking
	built-in. While updating a single ref is atomic, doing multiple operations is not."""
	def __init__(self, bucket_name, prefix = '.git'):
		self.bucket_name = bucket_name
		self.prefix = prefix
		self.client = boto3.client('s3')
		super(S3RefsContainer, self).__init__()

	def _calc_ref_path(self, ref):
		return "{}/{}".format(self.prefix, decode_bin(ref))

	def allkeys(self):
		path_prefix = '{}/refs'.format(self.prefix)
		sublen = len(path_prefix) - 4
		keys = self.client.list_objects(
			Bucket=self.bucket_name,
			Prefix=path_prefix
		)
		refs = [
			encode_bin(k['Key'][sublen:])
			for k in keys['Contents']
			if not k['Key'].endswith('/')
		]

		while keys['IsTruncated']:
			keys = self.client.list_objects(
				Bucket=self.bucket_name,
				Prefix=path_prefix,
				Marker=keys['NextMarker']
			)
			refs.append([
				encode_bin(k['Key'][sublen:])
				for k in keys['Contents']
				if not k['Key'].endswith('/')
			])

		if self.client.get_object(
				Bucket=self.bucket_name,
				Key=self._calc_ref_path(b'HEAD')):
			refs.append(b'HEAD')
		return refs

	def read_loose_ref(self, name):
		try:
			k = self.client.get_object(
				Bucket=self.bucket_name,
				Key=self._calc_ref_path(name)
			)
		except self.client.exceptions.NoSuchKey:
			return None

		return k['Body'].read()

	def get_packed_refs(self):
		return {}

	def set_symbolic_ref(self, name, other):
		sref = decode_bin(SYMREF) + decode_bin(other)
		log.debug('setting symbolic ref {} to {}'.format(decode_bin(name), sref))
		response = self.client.put_object(
			Bucket=self.bucket_name,
			Key=self._calc_ref_path(name),
			Body=sref
		)

	def set_if_equals(self, name, old_ref, new_ref):
		if old_ref is not None and self.read_loose_ref(name) != old_ref:
			return False

		realname, _ = self._follow(name)

		# set ref (set_if_equals is actually the low-level setting function)
		response = self.client.put_object(
			Bucket=self.bucket_name,
			Key=self._calc_ref_path(name),
			Body=new_ref
		)
		return True

	def add_if_new(self, name, ref):
		if None != self.read_loose_ref(name):
			return False

		self.set_if_equals(name, None, ref)
		return True

	def remove_if_equals(self, name, old_ref):
		try:
			response = self.client.get_object(
				Bucket=self.bucket_name,
				Key=self._calc_ref_path(name)
			)
		except self.client.exceptions.NoSuchKey:
			return None

		if old_ref is not None and response['Body'].read() != old_ref:
			return False

		self.client.delete_object(
			Bucket=self.bucket_name,
			Key=self._calc_ref_path(name)
		)
		return True


class S3ObjectStore(PackBasedObjectStore, S3PrefixFS):
	"""Storage backend on an Amazon S3 bucket.

	Stores objects on S3, replicating the path structure found usually on a "real"
	filesystem-based repository. Does not support packs."""

	def __init__(self, bucket_name, prefix = '.git', num_threads = 16):
		super(S3ObjectStore, self).__init__()
		self.bucket_name = bucket_name
		self.prefix = prefix
		self.uploader_threads = []
		self.work_queue = Queue()

		self.client = boto3.client('s3')
		self._pack_cache_time = 0

	def add_pack(self):
		fd, path = tempfile.mkstemp(suffix = ".pack")
		f = os.fdopen(fd, 'wb')

		def commit():
			try:
				os.fsync(fd)
				f.close()

				return self.upload_pack_file(path)
			finally:
				os.remove(path)
				log.debug('Removed temporary file {}'.format(path))
		return f, commit

	def _create_pack(self, path):
		def data_loader():
			# read and writable temporary file
			pack_tmpfile = tempfile.NamedTemporaryFile()

			# download into temporary file
			log.debug('Downloading pack {} into {}'.format(path, pack_tmpfile))
			k = self.client.get_object(
				Bucket=self.bucket_name,
				Key='{}/.pack'.format(path)
			)
			pack_tmpfile.write(k['Body'].read())
			log.debug('Filesize is {}'.format(k['ContentLength']))

			log.debug('Rewinding...')
			pack_tmpfile.flush()
			pack_tmpfile.seek(0)

			return PackData.from_file(pack_tmpfile, k['ContentLength'])

		def idx_loader():
			index_tmpfile = tempfile.NamedTemporaryFile()

			# download into temporary file
			log.debug('Downloading pack index {} into {}'.format(path, index_tmpfile))
			k = self.client.get_object(
				Bucket=self.bucket_name,
				Key='{}/.idx'.format(path)
			)
			index_tmpfile.write(k['Body'].read())
			log.debug('Filesize is {}'.format(k['ContentLength']))

			log.debug('Rewinding...')
			index_tmpfile.flush()
			index_tmpfile.seek(0)

			return PackData.from_file(index_tmpfile, k['ContentLength'])

		p = Pack(path)

		p._data_load = data_loader
		p._idx_load = idx_loader

		return p

	def contains_loose(self, sha):
		"""Check if a particular object is present by SHA1 and is loose."""
		try:
			obj = self.client.head_object(
				Bucket=self.bucket_name,
				Key=calc_object_path(self.prefix, sha)
			)
			return True
		except self.client.exceptions.NoSuchKey:
			return False

	def upload_pack_file(self, path):
		p = PackData(path)
		entries = p.sorted_entries()

		# get the sha1 of the pack, same method as dulwich's move_in_pack()
		pack_sha = iter_sha1(e[0] for e in entries)
		key_prefix = calc_pack_prefix(self.prefix, pack_sha)
		pack_key_name = '{}/.pack'.format(key_prefix)

		# FIXME: LOCK HERE? Possibly different pack files could
		#        have the same shas, depending on compression?

		log.debug('Uploading {} to {}'.format(path, pack_key_name))

		# set ref (set_if_equals is actually the low-level setting function)
		with open(path, 'r') as inputfile:
			response = self.client.put_object(
				Bucket=self.bucket_name,
				Key=pack_key_name,
				Body=inputfile
			)

		index_key_name = '{}/.idx'.format(key_prefix)
		index_fd, index_path = tempfile.mkstemp(suffix = '.idx')
		try:
			f = os.fdopen(index_fd, 'wb')
			write_pack_index_v2(f, entries, p.get_stored_checksum())
			os.fsync(index_fd)
			f.close()

			log.debug('Uploading {} to {}'.format(index_path, index_key_name))
			with open(index_path, 'r') as inputfile:
				response = self.client.put_object(
					Bucket=self.bucket_name,
					Key=index_key_name,
					Body=inputfile
				)

		finally:
			os.remove(index_path)

		p.close()

		return self._create_pack(key_prefix)

	def __iter__(self):
		return (k.name[-41:-39] + k.name[-38:] for k in self._s3_keys_iter())

	def _pack_cache_stale(self):
		# pack cache is valid for 5 minutes - no fancy checking here
		return time.time() - self._pack_cache_time > 5*60

	def _load_packs(self):
		packs = []

		# return pack objects, replace _data_load/_idx_load
		# when data needs to be fetched
		log.debug('Loading packs...')
		
		path_prefix = '{}/objects/pack/'.format(self.prefix)
		keys = self.client.list_objects(
			Bucket=self.bucket_name,
			Prefix=path_prefix
		)
		packs = [
			k['Key'][:-len('.pack')]
			for k in keys['Contents']
			if k['Key'].endswith('.pack')
		]

		while keys['IsTruncated']:
			keys = self.client.list_objects(
				Bucket=self.bucket_name,
				Prefix=path_prefix,
				Marker=keys['NextMarker']
			)
			packs.append([
				k['Key'][:-len('.pack')]
				for k in keys['Contents']
				if k['Key'].endswith('.pack')
			])

		self._pack_cache_time = time.time()
		return packs

	def _s3_keys_iter(self):
		path_prefix = '{}/objects/'.format(self.prefix)
		path_prefix_len = len(path_prefix)

		# valid keys look likes this: "path_prefix + 2 bytes sha1 digest + /
		#                              + remaining 38 bytes sha1 digest"
		valid_len = path_prefix_len + 2 + 1 + 38
		keys = self.client.list_objects(
			Bucket=self.bucket_name,
			Prefix=path_prefix
		)
		objects = [
			k['Key']
			for k in keys['Contents']
			if len(k['Key']) == valid_len
		]

		while keys['IsTruncated']:
			keys = self.client.list_objects(
				Bucket=self.bucket_name,
				Prefix=path_prefix,
				Marker=keys['NextMarker']
			)
			objects.append([
				k['Key']
				for k in keys['Contents']
				if len(k['Key']) == valid_len
			])

		return tuple(objects)

	def add_object(self, obj):
		"""Adds object the repository. Adding an object that already exists will
		   still cause it to be uploaded, overwriting the old with the same data."""
		self.add_objects([obj])


class S3CachedObjectStore(S3ObjectStore):
	def __init__(self, *args, **kwargs):
		super(S3CachedObjectStore, self).__init__(*args, **kwargs)
		self.cache = {}

	def __getitem__(self, name):
		if name in self.cache:
			log.debug('Cache hit on {}'.format(name))
			return self.cache[name]

		obj = super(S3CachedObjectStore, self).__getitem__(name)
		# do not store blobs
		if obj.get_type() == Blob.type_num:
			log.debug('Not caching Blob {}'.format(name))
		else:
			self.cache[obj.id] = obj

		return obj


class S3Repo(BaseRepo):
	"""A dulwich repository stored in an S3 bucket. Uses S3RefsContainer and S3ObjectStore
	as a backend. Does not do any sorts of locking, see documentation of S3RefsContainer
	and S3ObjectStore for details."""
	def __init__(self, bucket_name, prefix = '.git'):
		object_store = S3CachedObjectStore(bucket_name, prefix)
		refs = S3RefsContainer(bucket_name, prefix)

		# check if repo is initialized
		super(S3Repo, self).__init__(object_store, refs)

		try:
			log.debug('S3Repo with HEAD {}'.format(refs[b'HEAD']))
		except KeyError:
			self._init()

	def _init(self):
		log.debug('Initializing S3 repository')
		self.refs.set_symbolic_ref(b'HEAD', b'refs/heads/master')

	@classmethod
	def from_parsedurl(cls, parsedurl, **kwargs):
		prefix = ".git"
		if parsedurl.path:
			prefix = parsedurl.path + '/' + prefix
		return cls(bucket_name=parsedurl.hostname, prefix=prefix, **kwargs)

def calc_object_path(prefix, hexsha):
	path = '{}/objects/{}/{}'.format(prefix, decode_bin(hexsha[0:2]), decode_bin(hexsha[2:40]))
	return path

def calc_pack_prefix(prefix, hexsha):
	path = '{}/objects/pack/pack-{}'.format(prefix, decode_bin(hexsha))
	return path

def decode_bin(bin, encoding='utf8'):
	if isinstance(bin, bytes):
		return bin.decode(encoding)
	else:
		return bin

def encode_bin(bin, encoding='utf8'):
	if isinstance(bin, bytes):
		return bin
	else:
		return bin.encode(encoding)
