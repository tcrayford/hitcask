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

connect :: FilePath -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  let filepath = dir ++ "/current"
  m <- restoreFromFile filepath
  h <- openFile filepath ReadWriteMode
  t <- newTVarIO $! m
  return $! Hitcask t (CurrentLogFile h filepath) []

close :: Hitcask -> IO ()
close h = hClose (getHandle h)

