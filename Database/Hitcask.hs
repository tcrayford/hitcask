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
  logs <- openLogFiles dir
  h <- getOrCreateCurrent dir logs
  m <- restoreFromFile h
  t <- newTVarIO $! m
  curr <- reopen h
  return $! Hitcask t curr logs

reopen :: LogFile -> IO LogFile
reopen f = do
  h <- openFile (path f) ReadWriteMode
  return $! f { handle = h }

getOrCreateCurrent :: FilePath -> [LogFile] -> IO LogFile
getOrCreateCurrent dir logs = if null logs
    then openLogFile (dir ++ "/current.hitcask.data")
    else return $! head logs

close :: Hitcask -> IO ()
close h = hClose (getHandle h)

