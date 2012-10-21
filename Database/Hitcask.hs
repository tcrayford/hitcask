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
import Database.Hitcask.Logs

connect :: FilePath -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  m <- restoreFromLogDir dir
  logs <- openLogFiles dir
  h <- getOrCreateCurrent dir logs
  let allLogs = if null logs then [h] else logs
  t <- newTVarIO $! m
  l <- newTVarIO $! allLogs
  curr <- newTVarIO h
  return $! Hitcask t curr l dir

getOrCreateCurrent :: FilePath -> [LogFile] -> IO LogFile
getOrCreateCurrent dir logs
  | null logs = createNewLog dir
  | otherwise = return $! head logs

close :: Hitcask -> IO ()
close h = do
  logs <- readTVarIO $ files h
  mapM_ (hClose . handle) logs


