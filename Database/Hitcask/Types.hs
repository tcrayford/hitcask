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

type KeyDir = M.HashMap Key ValueLocation

data LogFile = LogFile {
    handle :: Handle
  , path :: FilePath
  }

type HintFile = LogFile

data MergingLog = MergingLog {
    mergedLog :: LogFile
  , originalFilePath :: FilePath
  , hintFile :: HintFile
  }

instance Show LogFile where
  show l = "(LogFile" ++  path l ++ ")"

instance Eq LogFile where
  l1 == l2 = path l1 == path l2

type CurrentLogFile = LogFile

data HitcaskSettings = HitcaskSettings {
    maxBytes :: Integer
  } deriving(Show, Eq)

data Hitcask = Hitcask {
    keys :: TVar KeyDir
  , current :: TVar CurrentLogFile
  , files :: TVar (M.HashMap FilePath LogFile)
  , dirPath :: FilePath
  , settings :: HitcaskSettings
  }

instance Show Hitcask where
  show db = "Hitcask: at " ++ dirPath db

type Key = ByteString
type Value = ByteString

getHandle :: Hitcask -> IO Handle
getHandle (Hitcask _ c _ _ _) = do
  (LogFile h _) <- readTVarIO c
  return $! h

