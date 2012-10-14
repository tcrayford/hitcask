module Database.Hitcask.Get where
import Database.Hitcask.Types
import Data.ByteString(ByteString)
import Control.Concurrent.STM
import qualified Data.HashMap.Strict as M
import System.IO
import qualified Data.ByteString.Char8 as B

get :: Hitcask -> ByteString -> IO (Maybe ByteString)
get h@(Hitcask t _ _) key = do
  m <- readTVarIO t
  let loc = M.lookup key m
  case loc of
    Just l -> readFromLocation h l key
    Nothing -> return Nothing

readFromLocation :: Hitcask -> ValueLocation -> ByteString -> IO (Maybe ByteString)
readFromLocation (Hitcask _ h _) (ValueLocation _ s p _) key = do
  hSeek h AbsoluteSeek p
  b <- B.hGet h (s + (4 * 4) + B.length key)
  return $! Just $! B.drop ((4 * 4) + B.length key) b

