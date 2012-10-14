module Database.Hitcask.Restore where
import Database.Hitcask.Types
import System.Directory
import qualified Data.HashMap.Strict as M
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get

restoreFromFile :: FilePath -> IO KeyDir
restoreFromFile f = do
  exists <- doesFileExist f
  if exists
    then do
      wholeFile <- B.readFile f
      let r = runParser wholeFile $
                untilM isEmpty (readLogEntry f (B.length wholeFile))
      return $! M.fromList (reverse r)
    else
      return $! M.empty

runParser :: ByteString -> Get a -> a
runParser input g = case runGet g input of
  Right r -> r
  Left e -> error e

readLogEntry :: FilePath -> Int -> Get (ByteString, ValueLocation)
readLogEntry f startingSize = do
  r <- remaining
  _ <- getWord32be --crc
  tstamp <- getWord32be
  keySize <- getWord32be
  vSize <- getWord32be
  key <- getByteString $ fromIntegral keySize
  _ <- getByteString $ fromIntegral vSize
  return (key, ValueLocation f (fromIntegral vSize) (fromIntegral (startingSize - r)) (fromIntegral tstamp))

untilM :: (Monad m) => m Bool -> m a -> m [a]
untilM f action = go []
  where go xs = do
                x <- f
                if not x
                  then do
                    y <- action
                    go (y:xs)
                  else return $! xs

