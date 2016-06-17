{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
module Database.Redis.Namespace
    ( RedisNS
    , runRedisNS
    , ttl
    , setnx
    , pttl
    , zrank
    , zremrangebyscore
    , hkeys
    , rpushx
    , debugObject
    , hlen
    , rpoplpush
    , brpop
    , zincrby
    , hgetall
    , hmset
    , sinter
    , pfadd
    , zremrangebyrank
    , sadd
    , lindex
    , lpush
    , smove
    , zscore
    , hdel
    , incrbyfloat
    , setbit
    , incrby
    , smembers
    , sunion
    , hvals
    , lpop
    , expire
    , mget
    , pexpire
    , renamenx
    , pfmerge
    , lrem
    , sdiff
    , get
    , getrange
    , sdiffstore
    , zcount
    , getset
    , dump
    , keys
    , rpush
    , hsetnx
    , mset
    , setex
    , psetex
    , scard
    , sunionstore
    , persist
    , strlen
    , lpushx
    , hset
    , brpoplpush
    , zrevrank
    , setrange
    , del
    , hincrbyfloat
    , hincrby
    , rpop
    , rename
    , zrem
    , hexists
    , decr
    , hmget
    , lrange
    , decrby
    , llen
    , append
    , incr
    , hget
    , pexpireat
    , ltrim
    , zcard
    , lset
    , expireat
    , move
    , getbit
    , msetnx
    , blpop
    , srem
    , sismember
    , set
    ) where

import qualified Control.Arrow
import           Control.Monad.Reader (ReaderT, runReaderT, ask, lift)
import           Data.ByteString      (ByteString)
import           Data.Monoid          ((<>))
import           Database.Redis       (RedisCtx, Status, runRedis)
import qualified Database.Redis as R

type RedisNS m f a = ReaderT ByteString m (f a)

runRedisNS :: R.Connection -> ByteString -> ReaderT ByteString R.Redis a -> IO a
runRedisNS conn ns = runRedis conn . flip runReaderT ns

join :: ByteString -> ByteString -> ByteString
join pre post = pre <> ":" <> post

prefix :: Monad m => ByteString -> ReaderT ByteString m ByteString
prefix post = join <$> ask <*> pure post


prefix1 :: Monad m
        => (ByteString -> m b)
        ->  ByteString -> ReaderT ByteString m b
prefix1 f key = prefix key >>= \k -> lift $ f k

prefix2 :: Monad m
        => (ByteString -> a -> m r)
        ->  ByteString -> a -> ReaderT ByteString m r
prefix2 f key a = prefix key >>= \k -> lift $ f k a

prefix3 :: Monad m
        => (ByteString -> a -> b -> m r)
        ->  ByteString -> a -> b -> ReaderT ByteString m r
prefix3 f key a b = prefix key >>= \k -> lift $ f k a b

prefixM1 :: Monad m
         => ([ByteString] -> m r)
         ->  [ByteString] -> ReaderT ByteString m r
prefixM1 f ks = mapM prefix ks >>= \k -> lift $ f k

prefixM2 :: Monad m
         => ([ByteString] -> a -> m r)
         ->  [ByteString] -> a -> ReaderT ByteString m r
prefixM2 f ks a = mapM prefix ks >>= \k -> lift $ f k a



ttl
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
ttl = prefix1 R.ttl

setnx
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ value
    -> RedisNS m f Bool
setnx = prefix2 R.setnx

pttl
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
pttl = prefix1 R.pttl

zrank
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ member
    -> RedisNS m f (Maybe Integer)
zrank = prefix2 R.zrank

zremrangebyscore
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Double -- ^ min
    -> Double -- ^ max
    -> RedisNS m f Integer
zremrangebyscore = prefix3 R.zremrangebyscore

hkeys
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f [ByteString]
hkeys = prefix1 R.hkeys

rpushx
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ value
    -> RedisNS m f Integer
rpushx = prefix2 R.rpushx

debugObject
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f ByteString
debugObject = prefix1 R.debugObject

hlen
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
hlen = prefix1 R.hlen

rpoplpush
    :: (RedisCtx m f)
    => ByteString -- ^ source
    -> ByteString -- ^ destination
    -> RedisNS m f (Maybe ByteString)
rpoplpush = prefix2 R.rpoplpush

brpop
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> Integer -- ^ timeout
    -> RedisNS m f (Maybe (ByteString,ByteString))
brpop = prefixM2 R.brpop

zincrby
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ increment
    -> ByteString -- ^ member
    -> RedisNS m f Double
zincrby = prefix3 R.zincrby

hgetall
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f [(ByteString,ByteString)]
hgetall = prefix1 R.hgetall

hmset
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [(ByteString,ByteString)] -- ^ fieldValue
    -> RedisNS m f Status
hmset = prefix2 R.hmset

sinter
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f [ByteString]
sinter = prefixM1 R.sinter

pfadd
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ value
    -> RedisNS m f Integer
pfadd = prefix2 R.pfadd

zremrangebyrank
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ start
    -> Integer -- ^ stop
    -> RedisNS m f Integer
zremrangebyrank = prefix3 R.zremrangebyrank

sadd
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ member
    -> RedisNS m f Integer
sadd = prefix2 R.sadd

lindex
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ index
    -> RedisNS m f (Maybe ByteString)
lindex = prefix2 R.lindex

lpush
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ value
    -> RedisNS m f Integer
lpush = prefix2 R.lpush

smove
    :: (RedisCtx m f)
    => ByteString -- ^ source
    -> ByteString -- ^ destination
    -> ByteString -- ^ member
    -> RedisNS m f Bool
smove = prefix3 R.smove

zscore
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ member
    -> RedisNS m f (Maybe Double)
zscore = prefix2 R.zscore

pfcount
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f Integer
pfcount = prefixM1 R.pfcount

hdel
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ field
    -> RedisNS m f Integer
hdel = prefix2 R.hdel

incrbyfloat
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Double -- ^ increment
    -> RedisNS m f Double
incrbyfloat = prefix2 R.incrbyfloat

setbit
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ offset
    -> ByteString -- ^ value
    -> RedisNS m f Integer
setbit = prefix3 R.setbit

incrby
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ increment
    -> RedisNS m f Integer
incrby = prefix2 R.incrby

smembers
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f [ByteString]
smembers = prefix1 R.smembers

sunion
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f [ByteString]
sunion = prefixM1 R.sunion

hvals
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f [ByteString]
hvals = prefix1 R.hvals

lpop
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f (Maybe ByteString)
lpop = prefix1 R.lpop

expire
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ seconds
    -> RedisNS m f Bool
expire = prefix2 R.expire

mget
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f [Maybe ByteString]
mget = prefixM1 R.mget

pexpire
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ milliseconds
    -> RedisNS m f Bool
pexpire = prefix2 R.pexpire

renamenx
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ newkey
    -> RedisNS m f Bool
renamenx k nk = do
  k'  <- prefix k
  nk' <- prefix nk
  lift $ R.renamenx k' nk'

pfmerge
    :: (RedisCtx m f)
    => ByteString -- ^ destkey
    -> [ByteString] -- ^ sourcekey
    -> RedisNS m f ByteString
pfmerge src dst = do
  src' <- prefix src
  dst' <- mapM prefix dst
  lift $ R.pfmerge src' dst'

lrem
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ count
    -> ByteString -- ^ value
    -> RedisNS m f Integer
lrem = prefix3 R.lrem

sdiff
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f [ByteString]
sdiff = prefixM1 R.sdiff

get
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f (Maybe ByteString)
get = prefix1 R.get

getrange
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ start
    -> Integer -- ^ end
    -> RedisNS m f ByteString
getrange = prefix3 R.getrange

sdiffstore
    :: (RedisCtx m f)
    => ByteString -- ^ destination
    -> [ByteString] -- ^ key
    -> RedisNS m f Integer
sdiffstore = prefix2 R.sdiffstore

zcount
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Double -- ^ min
    -> Double -- ^ max
    -> RedisNS m f Integer
zcount = prefix3 R.zcount

getset
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ value
    -> RedisNS m f (Maybe ByteString)
getset = prefix2 R.getset

dump
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f ByteString
dump = prefix1 R.dump

keys
    :: (RedisCtx m f)
    => ByteString -- ^ pattern
    -> RedisNS m f [ByteString]
keys = prefix1 R.keys

rpush
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ value
    -> RedisNS m f Integer
rpush = prefix2 R.rpush

hsetnx
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> ByteString -- ^ value
    -> RedisNS m f Bool
hsetnx = prefix3 R.hsetnx

mset
    :: (RedisCtx m f)
    => [(ByteString,ByteString)] -- ^ keyValue
    -> RedisNS m f Status
mset kv = lift . R.mset . flip map kv . Control.Arrow.first . join =<< ask

setex
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ seconds
    -> ByteString -- ^ value
    -> RedisNS m f Status
setex = prefix3 R.setex

psetex
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ milliseconds
    -> ByteString -- ^ value
    -> RedisNS m f Status
psetex = prefix3 R.psetex

scard
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
scard = prefix1 R.scard

sunionstore
    :: (RedisCtx m f)
    => ByteString -- ^ destination
    -> [ByteString] -- ^ key
    -> RedisNS m f Integer
sunionstore dst key = do
  dst' <- prefix dst
  key' <- mapM prefix key
  lift $ R.sunionstore dst' key'

persist
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Bool
persist = prefix1 R.persist

strlen
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
strlen = prefix1 R.strlen

lpushx
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ value
    -> RedisNS m f Integer
lpushx = prefix2 R.lpushx

hset
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> ByteString -- ^ value
    -> RedisNS m f Bool
hset = prefix3 R.hset

brpoplpush
    :: (RedisCtx m f)
    => ByteString -- ^ source
    -> ByteString -- ^ destination
    -> Integer -- ^ timeout
    -> RedisNS m f (Maybe ByteString)
brpoplpush src dst timeout = do
  src' <- prefix src
  dst' <- prefix dst
  lift $ R.brpoplpush src' dst' timeout

zrevrank
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ member
    -> RedisNS m f (Maybe Integer)
zrevrank = prefix2 R.zrevrank

setrange
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ offset
    -> ByteString -- ^ value
    -> RedisNS m f Integer
setrange = prefix3 R.setrange

del
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> RedisNS m f Integer
del = prefixM1 R.del

hincrbyfloat
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> Double -- ^ increment
    -> RedisNS m f Double
hincrbyfloat = prefix3 R.hincrbyfloat

hincrby
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> Integer -- ^ increment
    -> RedisNS m f Integer
hincrby = prefix3 R.hincrby

rpop
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f (Maybe ByteString)
rpop = prefix1 R.rpop

rename
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ newkey
    -> RedisNS m f Status
rename k nk = do
  k'  <- prefix k
  nk' <- prefix nk
  lift $ R.rename k' nk'

zrem
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ member
    -> RedisNS m f Integer
zrem = prefix2 R.zrem

hexists
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> RedisNS m f Bool
hexists = prefix2 R.hexists

decr
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
decr = prefix1 R.decr

hmget
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ field
    -> RedisNS m f [Maybe ByteString]
hmget = prefix2 R.hmget

lrange
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ start
    -> Integer -- ^ stop
    -> RedisNS m f [ByteString]
lrange = prefix3 R.lrange

decrby
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ decrement
    -> RedisNS m f Integer
decrby = prefix2 R.decrby

llen
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
llen = prefix1 R.llen

append
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ value
    -> RedisNS m f Integer
append = prefix2 R.append

incr
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
incr = prefix1 R.incr

hget
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ field
    -> RedisNS m f (Maybe ByteString)
hget = prefix2 R.hget

pexpireat
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ millisecondsTimestamp
    -> RedisNS m f Bool
pexpireat = prefix2 R.pexpireat

ltrim
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ start
    -> Integer -- ^ stop
    -> RedisNS m f Status
ltrim = prefix3 R.ltrim

zcard
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> RedisNS m f Integer
zcard = prefix1 R.zcard

lset
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ index
    -> ByteString -- ^ value
    -> RedisNS m f Status
lset = prefix3 R.lset

expireat
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ timestamp
    -> RedisNS m f Bool
expireat = prefix2 R.expireat

move
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ db
    -> RedisNS m f Bool
move = prefix2 R.move

getbit
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> Integer -- ^ offset
    -> RedisNS m f Integer
getbit = prefix2 R.getbit

msetnx
    :: (RedisCtx m f)
    => [(ByteString,ByteString)] -- ^ keyValue
    -> RedisNS m f Bool
msetnx kv = lift . R.msetnx . flip map kv . Control.Arrow.first . join =<< ask

blpop
    :: (RedisCtx m f)
    => [ByteString] -- ^ key
    -> Integer -- ^ timeout
    -> RedisNS m f (Maybe (ByteString,ByteString))
blpop = prefixM2 R.blpop

srem
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> [ByteString] -- ^ member
    -> RedisNS m f Integer
srem = prefix2 R.srem

sismember
    :: (RedisCtx m f)
    => ByteString -- ^ key
    -> ByteString -- ^ member
    -> RedisNS m f Bool
sismember = prefix2 R.sismember

set :: RedisCtx m f => ByteString -> ByteString -> RedisNS m f Status
set = prefix2 R.set

