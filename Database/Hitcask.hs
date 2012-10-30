module Database.Hitcask(
    get
  , put
  , delete
  , Hitcask()
  , connect
  , connectWith
  , close
  , compact
  , listKeys
  , standardSettings
) where
import Control.Concurrent.STM
import System.IO
import System.Directory
import Database.Hitcask.Types
import Database.Hitcask.Restore
import Database.Hitcask.Get
import Database.Hitcask.Put
import Database.Hitcask.Delete
import Database.Hitcask.Compact
import Database.Hitcask.Logs
import Database.Hitcask.ListKeys
import qualified Data.HashMap.Strict as M

standardSettings :: HitcaskSettings
standardSettings = HitcaskSettings (2 * 1073741824)

connect :: FilePath -> IO Hitcask
connect dir = connectWith dir standardSettings

connectWith :: FilePath -> HitcaskSettings -> IO Hitcask
connectWith dir options = do
  createDirectoryIfMissing True dir
  m <- restoreFromLogDir dir
  ls <- openLogFiles dir
  h@(LogFile _ p) <- getOrCreateCurrent dir ls
  let allLogs = if M.null ls then M.fromList [(p, h)] else ls
  t <- newTVarIO $! m
  l <- newTVarIO $! HitcaskLogs h allLogs
  return $! Hitcask t l dir options

getOrCreateCurrent :: FilePath -> M.HashMap FilePath LogFile -> IO LogFile
getOrCreateCurrent dir ls
  | M.null ls = createNewLog dir
  | otherwise = return $! head (M.elems ls)

close :: Hitcask -> IO ()
close h = do
  ls <- readTVarIO $ logs h
  mapM_ (hClose . handle) $ M.elems (files ls)

