module Database.Hitcask.Rotation where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import System.IO
import Control.Concurrent.STM
import qualified Data.HashMap.Strict as M
import Control.Monad(when)

maybeRotateCurrentFile :: Hitcask -> IO ()
maybeRotateCurrentFile h = do
  currentSize <- currentLogSize h
  when (currentSize > maxBytes (settings h))
    (rotateLogFile h)

currentLogSize :: Hitcask -> IO Integer
currentLogSize h = do
  f <- readTVarIO $ current h
  hFileSize $ handle f

rotateLogFile :: Hitcask -> IO ()
rotateLogFile h = do
  l <- createNewLog (dirPath h)
  swapLogFile h l

swapLogFile ::  Hitcask -> LogFile -> IO ()
swapLogFile h l@(LogFile _ p) = atomically $ do
    writeTVar (current h) l
    logs <- readTVar $ files h
    writeTVar (files h) (M.insert p l logs)


