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
import Database.Hitcask.Timestamp

connect :: FilePath -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  m <- restoreFromLogDir dir
  logs <- openLogFiles dir
  h <- getOrCreateCurrent dir logs
  t <- newTVarIO $! m
  return $! Hitcask t h logs

getOrCreateCurrent :: FilePath -> [LogFile] -> IO LogFile
getOrCreateCurrent dir logs = do
    t <- currentTimestamp
    if null logs
      then openLogFile (dir ++ "/" ++ show t ++ ".hitcask.data")
      else return $! head logs

close :: Hitcask -> IO ()
close h = hClose (getHandle h)

