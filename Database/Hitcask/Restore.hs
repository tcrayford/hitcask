module Database.Hitcask.Restore where
import Database.Hitcask.Types
import Database.Hitcask.Parsing
import qualified Data.HashMap.Strict as M
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.UTF8 as U
import Data.Serialize.Get
import Control.Monad(when)
import Data.Digest.CRC32
import Data.Word(Word32)
import Database.Hitcask.Logs
import System.IO
import System.Directory

restoreFromLogDir :: FilePath -> IO KeyDir
restoreFromLogDir dir = do
  ls <- openLogFiles dir
  restored <- mapM restoreFromFile $ M.elems ls
  return $! M.unions restored

restoreFromFile :: LogFile -> IO KeyDir
restoreFromFile f = do
  exists <- doesFileExist (path f ++ ".hint")
  if exists
    then restoreFromHintFile f
    else do
          r <- allKeys f
          return $! M.fromList (reverse r)

allKeys :: LogFile -> IO [(Key, ValueLocation)]
allKeys f = do
  let h = handle f
  hSeek h AbsoluteSeek 0
  wholeFile <- B.hGetContents h
  return $! parseMany wholeFile (readLogEntry (path f) (B.length wholeFile))

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


restoreFromHintFile :: HintFile -> IO KeyDir
restoreFromHintFile f = do
  let h = handle f
  wholeFile <- B.hGetContents h
  let r = parseMany wholeFile readLoc
  return $! M.fromList r

readLoc :: Get (Key, ValueLocation)
readLoc = do
  vs <- getInt32
  vp <- getInt32
  ts <- getInt32
  fpLength <- getInt32
  fp <- getByteString fpLength
  keyLength <- getInt32
  key <- getByteString keyLength
  return (key, ValueLocation (U.toString fp) vs (fromIntegral vp) (fromIntegral ts))

getInt32 :: Get Int
getInt32 = fmap fromIntegral getWord32be

