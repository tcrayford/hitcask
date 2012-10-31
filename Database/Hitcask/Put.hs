module Database.Hitcask.Put where
import Database.Hitcask.Types
import Database.Hitcask.Logs
import Database.Hitcask.Timestamp
import Database.Hitcask.Rotation
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8 as B
import Control.Concurrent.STM
import System.IO
import qualified Data.HashMap.Strict as M
import Data.Serialize.Put
import Data.Digest.CRC32

put :: Hitcask -> Key -> Value -> IO ()
put h key value = do
  maybeRotateCurrentFile h
  f <- readTVarIO $ logs h
  valueLocation <- writeValue (current f) key value
  updateKeyDir h key valueLocation
  flushLog (current f)

writeValue :: LogFile -> Key -> Value -> IO ValueLocation
writeValue l key value = do
  time <- currentTimestamp
  writeValueWithTimestamp l time key value

writeValueWithTimestamp :: LogFile -> Timestamp -> Key -> Value -> IO ValueLocation
writeValueWithTimestamp l@(LogFile f _) time key value = do
  hSeek f SeekFromEnd 0
  currentPosition <- hTell f
  let valueLocation = formatValue (path l) value currentPosition time
  appendToLog f key value valueLocation
  return $! valueLocation

updateKeyDir :: Hitcask -> Key -> ValueLocation -> IO ()
updateKeyDir h key valueLocation = atomically $
    modifyTVar' (keys h) $ \m ->
      M.insert key valueLocation m

formatValue :: FilePath -> Value -> Integer -> Timestamp -> ValueLocation
formatValue filePath value = ValueLocation filePath (B.length value)

appendToLog :: Handle -> Key -> Value -> ValueLocation -> IO ()
appendToLog h key value (ValueLocation _ _ _ t) = B.hPut h (formatForLog key value t)

putInt32 ::  Integral a => a -> Put
putInt32 a = putWord32be $ fromIntegral a

type LogEntry = ByteString
formatForLog :: Key -> Value -> Timestamp -> LogEntry
formatForLog k v t = runPut $ do
  putWord32be $ crc32 v
  putInt32 t
  putInt32 $ B.length k
  putInt32 $ B.length v
  putByteString k
  putByteString v

