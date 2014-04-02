module Main where

import System.Process
import System.Environment
import System.Directory
import Text.Printf
import Control.Monad
import Data.List

redisInstances :: Int
redisInstances = 128

workingDir :: String
workingDir = "/home/b/run/redis/"

pidFile :: Int -> String
pidFile i = printf "%03i" i ++ ".pid"

rdbFile :: Int -> String
rdbFile i = printf "%03i" i ++ ".rdb"

main = do
  cmd:_ <- getArgs
  system $ "mkdir -p " ++ workingDir
  case cmd of
    "start" -> forM_ [0..redisInstances-1] (system . redis_server)
    "stop" -> forM_ [0..redisInstances-1] kill
    _ -> return ()
  return ()

redis_server :: Int -> String
redis_server i = intercalate " " args where
  args = [ "redis-server"
         , "--daemonize yes"
         , "--port", show (6379 + i)
         , "--pidfile", pidFile i
         , "--dbfilename", rdbFile i
         , "--dir", workingDir
         ]

kill :: Int -> IO ()
kill i = do
  tf <- doesFileExist pidf
  case tf of
    False -> putStrLn $ "pid file [" ++ pidf ++ "] doesn't exist"
    True -> do
      pid <- readFile pidf
      system $ "kill " ++ pid
      return ()
  where
    pidf = workingDir ++ pidFile i
