module Database.Hitcask.Logs where
import Database.Hitcask.Types
import System.FilePath.Glob
import Data.List(sortBy)
import Data.List.Split(splitOn)
import Data.Ord(comparing)
import System.IO
import Database.Hitcask.Timestamp
import qualified Data.HashMap.Strict as M

logFilesInDir :: FilePath -> IO [FilePath]
logFilesInDir dir = do
  (matched, _) <- globDir [compile "*.hitcask.data"] dir
  (merged, _) <- globDir [compile "*.hitcask.data.merged"] dir
  return $! byRecent $ concat matched ++ concat merged

byRecent :: [FilePath] -> [FilePath]
byRecent = sortBy mostRecent

mostRecent :: FilePath -> FilePath -> Ordering
mostRecent a b = descending $ comparing getTimestamp a b

descending :: Ordering -> Ordering
descending EQ = EQ
descending GT = LT
descending LT = GT

getTimestamp :: FilePath -> Integer
getTimestamp = read . head . splitOn "." . last . splitOn "/"

openLogFiles :: FilePath -> IO (M.HashMap FilePath LogFile)
openLogFiles dir = do
  filenames <- logFilesInDir dir
  ls <- mapM openLogFile filenames
  return $! M.fromList $ zip filenames ls

openLogFile :: FilePath -> IO LogFile
openLogFile filepath = do
  h <- openFile filepath ReadWriteMode
  return $! LogFile h filepath

createNewLog :: FilePath -> IO LogFile
createNewLog dir = do
    t <- currentTimestamp
    openLogFile (dir ++ "/" ++ show t ++ ".hitcask.data")

flushLog :: LogFile -> IO ()
flushLog = hFlush . handle

closeLog :: LogFile -> IO ()
closeLog = hClose . handle
