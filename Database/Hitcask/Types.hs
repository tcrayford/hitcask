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
  }

type KeyDir = M.HashMap ByteString ValueLocation

data Hitcask = Hitcask {
    keys :: TVar KeyDir
  , currentFile :: Handle
  , fileLocation :: FilePath
  }

