module Database.Hitcask.Put where
import Database.Hitcask.Types
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Time.Clock.POSIX
import Control.Concurrent.STM
import System.IO
import qualified Data.HashMap.Strict as M
import Data.Serialize.Put
import Data.Digest.CRC32

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

formatValue :: FilePath -> ByteString -> Integer -> POSIXTime -> ValueLocation
formatValue filePath value filePos t = ValueLocation filePath (B.length value) filePos (round t)

appendToLog :: Handle -> ByteString -> ByteString -> ValueLocation -> IO ()
appendToLog h key value (ValueLocation _ _ _ t) = do
   hSeek h SeekFromEnd 0
   B.hPut h (formatForLog key value t)

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

