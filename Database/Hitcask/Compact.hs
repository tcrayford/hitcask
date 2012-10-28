module Database.Hitcask.Compact where
import Database.Hitcask.Types
import Database.Hitcask.Restore
import Database.Hitcask.Put
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
  (x,y) <- atomically $ do
    a <- readTVar $ current db
    b <- readTVar $ files db
    return (a,b)
  return $! filter (not . (== x)) (M.elems y)

compactLogFile :: LogFile -> IO (LogFile, KeyDir)
compactLogFile l = do
  currentContent <- readContent l
  writeMergedContent l currentContent

readContent :: LogFile -> IO (M.HashMap Key Value)
readContent f = do
  let h = handle f
  hSeek h AbsoluteSeek 0
  wholeFile <- getFileContents f
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

writeMergedContent :: LogFile -> M.HashMap Key Value -> IO (LogFile, KeyDir)
writeMergedContent l ks = do
  newLog <- createMergedLog l
  r <- mapM (appendToLog' newLog) (M.toList ks)
  return (newLog, M.fromList r)

appendToLog' :: LogFile -> (Key, Value) -> IO (Key, ValueLocation)
appendToLog' l (key, value) = do
  loc <- writeValue l key value
  return (key, loc)

createMergedLog :: LogFile -> IO LogFile
createMergedLog (LogFile _ p) = openLogFile (p ++ ".merged")

getFileContents :: LogFile -> IO B.ByteString
getFileContents (LogFile h _) = do
  s <- hFileSize h
  hSeek h AbsoluteSeek 0
  B.hGetNonBlocking h (fromIntegral s)

replaceNonActive :: Hitcask -> [(LogFile, KeyDir)] -> IO ()
replaceNonActive db s = atomically $ mapM_ (swapInLog db) s

swapInLog :: Hitcask -> (LogFile, KeyDir) -> STM ()
swapInLog db (l@(LogFile _ p), mergedKeys) = do
  modifyTVar (files db) $ \m ->
    M.insert (originalFilename p) l m
  modifyTVar (keys db) $ \m ->
    addMergedKeyDir m mergedKeys

addMergedKeyDir :: KeyDir -> KeyDir -> KeyDir
addMergedKeyDir = M.unionWith latestWrite

latestWrite :: ValueLocation -> ValueLocation -> ValueLocation
latestWrite v1 v2
  | timestamp v1 > timestamp v2 = v1
  | otherwise = v2

originalFilename :: FilePath -> FilePath
originalFilename f = take (length f - length ".merged") f

