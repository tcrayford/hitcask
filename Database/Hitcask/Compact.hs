module Database.Hitcask.Compact where
import Database.Hitcask.Types
import Database.Hitcask.Restore
import Database.Hitcask.Parsing
import Database.Hitcask.Util
import Database.Hitcask.Put
import Database.Hitcask.Hint
import Database.Hitcask.Logs
import Control.Concurrent.STM
import qualified Data.HashMap.Strict as M
import System.IO
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get

compact :: Hitcask -> IO ()
compact db = do
  nonActive <- allNonActive db
  merged <- mapM compactLogFile nonActive
  replaceNonActive db merged

allNonActive :: Hitcask -> IO [LogFile]
allNonActive db = do
  x <- readTVarIO $ logs db
  return $! nonActive x

nonActive :: HitcaskLogs -> [LogFile]
nonActive l = remove (current l) (M.elems (files l))

compactLogFile :: LogFile -> IO (MergingLog, KeyDir)
compactLogFile l = do
  currentContent <- readState l
  writeMergedContent l currentContent

readState :: LogFile -> IO (M.HashMap Key Value)
readState f = do
  let h = handle f
  hSeek h AbsoluteSeek 0
  wholeFile <- readImmutableLog f
  let r = runParser wholeFile $
        untilM isEmpty readLogEntry'
  return $! M.fromList (reverse r)

readLogEntry' :: Get (Key, Value)
readLogEntry' = do
  _ <- remaining
  crc <- getWord32be --crc
  _ <- getWord32be
  keySize <- getWord32be
  vSize <- getWord32be
  key <- getByteString $ fromIntegral keySize
  value <- getByteString $ fromIntegral vSize
  checkValue value crc
  return (key, value)

writeMergedContent :: LogFile -> M.HashMap Key Value -> IO (MergingLog, KeyDir)
writeMergedContent l ks = do
  newLog <- createMergedLog l
  r <- mapM (appendToLog' newLog) (M.toList ks)
  return (newLog, M.fromList r)

appendToLog' :: MergingLog -> (Key, Value) -> IO (Key, ValueLocation)
appendToLog' (MergingLog l _ h) (key, value) = do
  loc <- writeValue l key value
  writeHint h key loc
  return (key, loc)

createMergedLog :: LogFile -> IO MergingLog
createMergedLog (LogFile _ p) = do
  l <- openLogFile (p ++ ".merged")
  h <- createHintFile p
  return $! MergingLog l p h

readImmutableLog :: LogFile -> IO B.ByteString
readImmutableLog (LogFile h _) = do
  s <- hFileSize h
  hSeek h AbsoluteSeek 0
  B.hGetNonBlocking h (fromIntegral s)

replaceNonActive :: Hitcask -> [(MergingLog, KeyDir)] -> IO ()
replaceNonActive db s = atomically $ mapM_ (swapInLog db) s

swapInLog :: Hitcask -> (MergingLog, KeyDir) -> STM ()
swapInLog db (MergingLog l original _, mergedKeys) = do
  modifyTVar (logs db) $ \m ->
    m { files = M.insert original l (files m) }
  modifyTVar (keys db) $ \m ->
    addMergedKeyDir m mergedKeys

addMergedKeyDir :: KeyDir -> KeyDir -> KeyDir
addMergedKeyDir = M.unionWith latestWrite

latestWrite :: ValueLocation -> ValueLocation -> ValueLocation
latestWrite v1 v2
  | timestamp v1 > timestamp v2 = v1
  | otherwise = v2

