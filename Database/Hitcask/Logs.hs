module Database.Hitcask.Logs where
import Database.Hitcask.Types
import System.FilePath.Glob
import Data.List(sortBy)
import Data.List.Split(splitOn)
import Data.Ord(comparing)
import System.IO
import Database.Hitcask.Timestamp

logFilesInDir :: FilePath -> IO [FilePath]
logFilesInDir dir = do
  (matched, _) <- globDir [compile "*.hitcask.data"] dir
  return $! sortBy mostRecent (concat matched)

mostRecent :: FilePath -> FilePath -> Ordering
mostRecent = comparing getTimestamp

getTimestamp :: FilePath -> Int
getTimestamp = read . head . splitOn "."

openLogFiles :: FilePath -> IO [LogFile]
openLogFiles dir = do
  filenames <- logFilesInDir dir
  mapM openLogFile filenames

openLogFile :: FilePath -> IO LogFile
openLogFile filepath = do
  h <- openFile filepath ReadWriteMode
  return $! LogFile h filepath

createNewLog :: FilePath -> IO LogFile
createNewLog dir = do
    t <- currentTimestamp
    openLogFile (dir ++ "/" ++ show t ++ ".hitcask.data")

