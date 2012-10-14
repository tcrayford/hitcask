module Database.Hitcask(
    get
  , put
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

connect :: String -> IO Hitcask
connect dir = do
  createDirectoryIfMissing True dir
  let filepath = dir ++ "/current"
  m <- restoreFromFile filepath
  h <- openFile filepath ReadWriteMode
  t <- newTVarIO $! m
  return $! Hitcask t h filepath

close :: Hitcask -> IO ()
close (Hitcask _ h _) = hClose h

