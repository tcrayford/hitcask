module Database.Hitcask.Rotation where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import System.IO
import Control.Concurrent.STM

maybeRotateCurrentFile :: Hitcask -> IO Hitcask
maybeRotateCurrentFile h = do
  currentSize <- currentLogSize h
  if currentSize > 5
    then rotateLogFile h
    else return $! h

currentLogSize :: Hitcask -> IO Integer
currentLogSize h = do
  f <- getHandle h
  hFileSize f

rotateLogFile :: Hitcask -> IO Hitcask
rotateLogFile h = do
  l <- createNewLog (dirPath h)
  atomically $ do
    writeTVar (current h) l
    logs <- readTVar $ files h
    writeTVar (files h) (l:logs)
  return $! h

