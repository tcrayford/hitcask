module Database.Hitcask(
    get
  , put
  , delete
  , Hitcask()
  , connect
  , close
) where
import Control.Concurrent.STM
import System.IO
import System.Directory
import Database.Hitcask.Types
import Database.Hitcask.Restore
import Database.Hitcask.Get
import Database.Hitcask.Put
import Database.Hitcask.Delete
import System.FilePath.Glob
import Data.List(sortBy)
import Data.List.Split(splitOn)
import Data.Ord(comparing)

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

connect :: FilePath -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  let filepath = dir ++ "/current.hitcask.data"
  m <- restoreFromFile filepath
  t <- newTVarIO $! m
  logs <- openLogFiles dir
  h <- getOrCreateCurrent dir logs
  return $! Hitcask t h logs

getOrCreateCurrent :: FilePath -> [LogFile] -> IO LogFile
getOrCreateCurrent dir logs = if null logs
    then openLogFile (dir ++ "/current.hitcask.data")
    else return $! head logs

close :: Hitcask -> IO ()
close h = hClose (getHandle h)

