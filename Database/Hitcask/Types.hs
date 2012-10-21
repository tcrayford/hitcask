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
  , current :: CurrentLogFile
  , files :: [LogFile]
  }

type Key = ByteString
type Value = ByteString

getHandle :: Hitcask -> Handle
getHandle (Hitcask _ (LogFile h _) _) = h

