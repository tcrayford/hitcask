{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Get where
import Database.Hitcask.Types
import Control.Concurrent.STM
import qualified Data.HashMap.Strict as M
import System.IO
import qualified Data.ByteString.Char8 as B
import Control.Monad(liftM)

get :: Hitcask -> Key -> IO (Maybe Value)
get h key = liftM removeDeletion $ readValue h key

removeDeletion :: Maybe Value -> Maybe Value
removeDeletion j@(Just v)
  | v == "<<hitcask_tombstone>>" = Nothing
  | otherwise = j
removeDeletion x = x

readValue :: Hitcask -> Key -> IO (Maybe Value)
readValue h key = do
  m <- readTVarIO (keys h)
  let loc = M.lookup key m
  case loc of
    Just l -> readFromLocation h l key
    Nothing -> return Nothing

logForValueLocation :: M.HashMap FilePath LogFile -> ValueLocation -> Maybe LogFile
logForValueLocation ls (ValueLocation file _ _ _) = M.lookup file ls

readFromLocation :: Hitcask -> ValueLocation -> Key -> IO (Maybe Value)
readFromLocation c v key = do
  f <- readTVarIO $ logs c
  let l = logForValueLocation (files f) v
  case l of
    (Just (LogFile h _)) -> readFromLogFile h v key
    Nothing -> return Nothing

readFromLogFile :: Handle -> ValueLocation -> Key -> IO (Maybe Value)
readFromLogFile h (ValueLocation _ s p _) key = do
  hSeek h AbsoluteSeek p
  b <- B.hGet h (s + (4 * 4) + B.length key)
  return $! Just $! B.drop ((4 * 4) + B.length key) b

