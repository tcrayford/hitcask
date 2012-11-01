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
import System.Directory
import qualified Data.ByteString.Char8 as B
import Data.Serialize.Get

compact :: Hitcask -> IO ()
compact db = do
  immutable <- allNonActive db
  merged <- mapM compactLogFile immutable
  replaceNonActive db merged
  removeAlreadyMerged immutable

allNonActive :: Hitcask -> IO [LogFile]
allNonActive db = fmap nonActive . readTVarIO $ logs db

nonActive :: HitcaskLogs -> [LogFile]
nonActive l = remove (current l) (M.elems (files l))

compactLogFile :: LogFile -> IO (MergingLog, KeyDir)
compactLogFile l = do
  currentContent <- readState l
  x <- writeMergedContent l currentContent
  closeHint l
  return $! x

readState :: LogFile -> IO (M.HashMap Key (Timestamp, Value))
readState f = do
  let h = handle f
  hSeek h AbsoluteSeek 0
  wholeFile <- readImmutableLog f
  let r = parseMany wholeFile readLogEntry'
  return $! M.fromList (reverse r)

readLogEntry' :: Get (Key, (Timestamp, Value))
readLogEntry' = do
  _ <- remaining
  crc <- getWord32be --crc
  ts <- getInt32
  keySize <- getWord32be
  vSize <- getWord32be
  key <- getByteString $ fromIntegral keySize
  value <- getByteString $ fromIntegral vSize
  checkValue value crc
  return (key, (ts, value))

writeMergedContent :: LogFile -> M.HashMap Key (Int, Value) -> IO (MergingLog, KeyDir)
writeMergedContent l ks = do
  newLog <- createMergedLog l
  r <- mapM (appendToLog' newLog) (M.toList ks)
  return (newLog, M.fromList r)

appendToLog' :: MergingLog -> (Key, (Timestamp, Value)) -> IO (Key, ValueLocation)
appendToLog' (MergingLog l _ h) (key, (time, value)) = do
  loc <- writeValueWithTimestamp l time key value
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
swapInLog db (l, mergedKeys) = do
  modifyTVar (logs db) (addMergedLog l)
  modifyTVar (keys db) $ \m ->
    addMergedKeyDir m mergedKeys

addMergedLog :: MergingLog -> HitcaskLogs -> HitcaskLogs
addMergedLog newLog = removeMergedLog newLog . addNewLog newLog

removeMergedLog :: MergingLog -> HitcaskLogs -> HitcaskLogs
removeMergedLog l ls = ls { files = M.delete (originalFilePath l) (files ls) }

addNewLog :: MergingLog -> HitcaskLogs -> HitcaskLogs
addNewLog l ls = ls { files = M.insert (path $ mergedLog l)
                                       (mergedLog l)
                                       (files ls) }

addMergedKeyDir :: KeyDir -> KeyDir -> KeyDir
addMergedKeyDir = M.unionWith latestWrite

latestWrite :: ValueLocation -> ValueLocation -> ValueLocation
latestWrite v1 v2
  | timestamp v1 > timestamp v2 = v1
  | otherwise = v2

removeAlreadyMerged :: [LogFile] -> IO ()
removeAlreadyMerged = mapM_ kill
  where kill x = do
          hClose $ handle x
          removeFile $ path x

