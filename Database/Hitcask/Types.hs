module Database.Hitcask.Types where
import Control.Concurrent.STM
import qualified Data.HashMap.Strict as M
import Data.ByteString(ByteString)
import System.IO(Handle)

data ValueLocation = ValueLocation {
      fileId :: FilePath
    , valueSize :: Int
    , valuePos :: Integer
    , timestamp :: Integer
  } deriving (Eq, Show)

type KeyDir = M.HashMap ByteString ValueLocation

data LogFile = LogFile {
    handle :: Handle
  , path :: FilePath
  }

type CurrentLogFile = LogFile

data Hitcask = Hitcask {
    keys :: TVar KeyDir
  , current :: TVar CurrentLogFile
  , files :: TVar [LogFile]
  }

type Key = ByteString
type Value = ByteString

getHandle :: Hitcask -> IO Handle
getHandle (Hitcask _ c _) = do
  (LogFile h _) <- atomically $ readTVar c
  return $! h
