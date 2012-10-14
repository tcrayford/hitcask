module Database.Hitcask where
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict as M
import Control.Concurrent.STM
import System.IO
import System.Directory
import Data.Time.Clock.POSIX
import Data.Digest.CRC32
import Data.Serialize.Put
import Database.Hitcask.Types
import Database.Hitcask.Restore

connect :: String -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  let filepath = dir ++ "/current"
  m <- restoreFromFile filepath
  h <- openFile filepath ReadWriteMode
  t <- newTVarIO $! m
  return $! Hitcask t h filepath

close :: Hitcask -> IO ()
close (Hitcask _ h _) = hClose h

get :: Hitcask -> ByteString -> IO (Maybe ByteString)
get h@(Hitcask t _ _) key = do
  m <- readTVarIO t
  let loc = M.lookup key m
  case loc of
    Just l -> readFromLocation h l key
    Nothing -> return Nothing

readFromLocation :: Hitcask -> ValueLocation -> ByteString -> IO (Maybe ByteString)
readFromLocation (Hitcask _ h _) (ValueLocation _ s p _) key = do
  x <- hIsClosed h
  if x
    then putStrLn "handle closed: readFromLocation"
    else putStr ""
  hSeek h AbsoluteSeek p
  b <- B.hGet h (s + (4 * 4) + B.length key)
  return $! Just $! B.drop ((4 * 4) + B.length key) b

appendToLog :: Handle -> ByteString -> ByteString -> ValueLocation -> IO ()
appendToLog h key value (ValueLocation _ _ _ t) = B.hPut h (formatForLog key value t)

putInt32 ::  Integral a => a -> Put
putInt32 a = putWord32be $ fromIntegral a

formatForLog :: ByteString -> ByteString -> Integer -> ByteString
formatForLog k v t = runPut $ do
  putWord32be $ crc32 v
  putInt32 t
  putInt32 $ B.length k
  putInt32 $ B.length v
  putByteString k
  putByteString v

formatValue :: FilePath -> ByteString -> Integer -> POSIXTime -> ValueLocation
formatValue filePath value filePos t = ValueLocation filePath (B.length value) filePos (round t)

put :: Hitcask -> ByteString -> ByteString -> IO Hitcask
put h@(Hitcask t f filename) key value = do
  currentPosition <- hTell f
  time <- getPOSIXTime
  let valueLocation = formatValue filename value currentPosition time
  b <- atomically $ do
    modifyTVar' t $ \m ->
      M.insert key valueLocation m
    return $! h
  appendToLog f key value valueLocation
  return $! b
