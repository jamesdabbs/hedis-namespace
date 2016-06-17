{-# LANGUAGE OverloadedStrings #-}
module Main where

import Database.Redis.Namespace

import           Control.Monad.Reader (liftIO)
import           Data.ByteString      (ByteString)
import qualified Database.Redis       as R

main :: IO ()
main = do
  conn <- R.connect R.defaultConnectInfo
  R.runRedis conn $ do
    s <- R.get "ns:floop"
    liftIO $ case s of
      Left  _ -> print "left"
      Right r -> print r

    R.set "ns:floop" "zxcv"
    v <- R.get "ns:floop"
    liftIO $ case v of
      Left  _ -> print "left"
      Right r -> print r

  runRedisNS conn "ns" $ do
    v <- get "floop"
    liftIO $ case v of
      Left  _ -> print "left"
      Right r -> print (r :: Maybe ByteString)
