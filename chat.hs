-- ghc -O2 -threaded -rtsopts -eventlog chat
-- ghc -O2 -threaded -rtsopts -prof chat
-- ./chat 100 +RTS -la -N16 -A1G
-- weighttp -k -t 8 -c 200 -n 100000 http://192.168.1.2:3001/chats/ch1
-- redis-benchmark -h 192.168.1.2 -c 100 -n 10000 zrangebyscore ch1 -Inf +Inf
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Blaze.ByteString.Builder (copyByteString, Builder)
import           Control.Concurrent
import           Control.Exception
import           Control.Monad (void, forever)
import           Control.Monad.IO.Class
import           Data.Bits
import           Data.Digest.Pure.SHA
import           Data.Int
import           Data.Monoid
import           Data.Text (unpack)
import           Data.Time
import           Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)
import           Data.Time.Format
import           Data.Vector as DV hiding ((++), map)
import           Data.Word
import           Database.Redis as R
import           Debug.Trace
import           Network.HTTP.Types.Status (status500, status200)
import           Network.Wai
import           Network.Wai.Handler.Warp
import           System.Environment
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import qualified Data.Conduit as C
import qualified Data.Conduit.List as CL

t = main

redisInstances :: Int
redisInstances = 128

p :: String -> C.ResourceT IO ()
p = liftIO . putStrLn

infinity :: Double
infinity = read "Infinity"

ninfinity :: Double
ninfinity = read "-Infinity"

newline :: BC.ByteString
newline = BC.singleton '\n'

epochNow :: IO Double
epochNow = getCurrentTime >>= utcToEpoch where
  utcToEpoch :: UTCTime -> IO Double
  utcToEpoch = return . fromIntegral . round . utcTimeToPOSIXSeconds

main :: IO ()
main = do
  putStrLn "starting chat"
  connsPerInstance:_ <- getArgs
  conns <- generateM redisInstances $
           \i -> connect defaultConnectInfo {
                   connectMaxConnections = read connsPerInstance
                 , connectPort = PortNumber $ 6379 + fromIntegral i
                 }
  run 3001 $ app $ preShardRedis conns
  --run 3001 noRedis

--preShardRedis :: Vector R.Connection -> BLC.ByteString -> Redis a -> IO a
preShardRedis conns bs = runRedis conn
{-preShardRedis conns bs cmd = do
  putStrLn $ "shard " ++ show shard
  runRedis conn cmd-}
  where
    shard :: Word64
    shard = ringSeg (fromIntegral redisInstances) bs

    conn :: R.Connection
    conn = conns ! fromIntegral shard

connLostH :: ConnectionLostException -> IO (Either Reply [BC.ByteString])
connLostH e = do
  putStrLn $ "CAUGHT " ++ show e
  return $ Left $ Error BC.empty

ioH :: IOException -> IO (Either Reply [BC.ByteString])
ioH e = do
  putStrLn $ "CAUGHT " ++ show e
  return $ Left $ Error BC.empty

someH :: SomeException -> IO (Either Reply [BC.ByteString])
someH e = do
  putStrLn $ "CAUGHT " ++ show e
  return $ Left $ Error BC.empty

handlers :: [Handler (Either Reply [BC.ByteString])]
handlers = [Handler connLostH, Handler ioH, Handler someH]


{-app :: RedisCtx m f
    => (BLC.ByteString -> m (f [BC.ByteString]) -> IO (Either Reply [BC.ByteString]))
    -> Application-}
--app :: (BLC.ByteString -> Redis a -> IO a) -> Application
app runRedis req = do
  --p $ show $ rawPathInfo req
  case map unpack (pathInfo req) of
    ("chat":ch:msg:[]) -> do
      liftIO $ do
        now <- epochNow
        --runRedis (BLC.pack ch) $ zadd (BC.pack ch) [(now,BC.pack msg)]
        return ()
      return builderNoLen
    ("chats":ch:[]) -> do
      either <- liftIO $ do
        tid <- myThreadId
        now <- epochNow
        traceEventIO $ "zrangebyscore BEGIN " ++ show tid
        (either :: Either Reply [BC.ByteString]) <- (runRedis (BLC.pack ch) $ zrangebyscore (BC.pack ch) ninfinity infinity) `catches` handlers
        traceEventIO $ "zrangebyscore END " ++ show tid
        return either
      case either of
        Left err -> res status500 [("Content-Type", "text/plain")] $ copyByteString BC.empty
        Right bs -> res status200 [("Content-Type", "text/plain")] $ mconcat $ map (copyByteString . flip BC.append newline) bs
    _ -> return builderNoLen

noRedis :: Application
noRedis req = return builderNoLen

--res :: (Monad m) => Status -> ReponseHeaders -> Builder -> m Response
res s h b = return $ responseBuilder s h b

builderNoLen = responseBuilder
    status200
    [ ("Content-Type", "text/plain")
    ]
    $ copyByteString "PONG"


ringSeg :: Word64 -- ^ segments
        -> BLC.ByteString -- ^ some identifier
        -> Word64
ringSeg segments something = ringPos something `mod` segments
  
ringPos :: BLC.ByteString -> Word64
ringPos bs = toWord64 lbs `xor` toWord64 rbs
  where
    bs' = bytestringDigest $ sha1 bs
    len :: Double = (fromIntegral $ BLC.length bs') / 2
    (lbs, rbs) = BLC.splitAt (fromIntegral len) bs'

toWord64 :: BL.ByteString -> Word64
toWord64 bs = BL.foldl' addShift 0 bs
  where addShift n y = (n `shiftL` 8) .|. fromIntegral y

instance Integral Float where
  quotRem a b = (fab, (ab - fab)*b)
    where
      ab = a/b
      fab = floor ab
  toInteger = floor

instance Integral Double where
  quotRem a b = (fab, (ab - fab)*b)
    where
      ab = a/b
      fab = floor ab
  toInteger = floor
