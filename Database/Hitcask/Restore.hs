module Database.Hitcask.Restore where
import Database.Hitcask.Types
import qualified Data.HashMap.Strict as M
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get
import Control.Monad(when)
import Data.Digest.CRC32
import Data.Word(Word32)
import Database.Hitcask.Logs

restoreFromLogDir :: FilePath -> IO KeyDir
restoreFromLogDir dir = do
  logs <- openLogFiles dir
  restored <- mapM restoreFromFile logs
  return $! M.unions (reverse restored)

restoreFromFile :: LogFile -> IO KeyDir
restoreFromFile f = do
  wholeFile <- B.hGetContents (handle f)
  let r = runParser wholeFile $
            untilM isEmpty (readLogEntry (path f) (B.length wholeFile))
  return $! M.fromList (reverse r)

runParser :: ByteString -> Get a -> a
runParser input g = case runGet g input of
  Right r -> r
  Left e -> error e

readLogEntry :: FilePath -> Int -> Get (Key, ValueLocation)
readLogEntry f startingSize = do
  r <- remaining
  crc <- getWord32be --crc
  tstamp <- getWord32be
  keySize <- getWord32be
  vSize <- getWord32be
  key <- getByteString $ fromIntegral keySize
  value <- getByteString $ fromIntegral vSize
  checkValue value crc
  return (key, ValueLocation f (fromIntegral vSize) (fromIntegral (startingSize - r)) (fromIntegral tstamp))

checkValue :: (Monad m) => Value -> Word32 -> m ()
checkValue v crc = when (crc32 v /= crc)
  (fail $ "value failed crc check: " ++ show v)

untilM :: (Monad m) => m Bool -> m a -> m [a]
untilM f action = go []
  where go xs = do
                x <- f
                if not x
                  then do
                    y <- action
                    go (y:xs)
                  else return $! xs

