module Database.Hitcask(
    get
  , put
  , delete
  , Hitcask()
  , connect
  , close
  , compact
  , listKeys
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
connect dir = do
  createDirectoryIfMissing True dir
  m <- restoreFromLogDir dir
  logs <- openLogFiles dir
  h@(LogFile _ p) <- getOrCreateCurrent dir logs
  let allLogs = if M.null logs then M.fromList [(p, h)] else logs
  t <- newTVarIO $! m
  l <- newTVarIO $! allLogs
  curr <- newTVarIO h
  return $! Hitcask t curr l dir standardSettings

getOrCreateCurrent :: FilePath -> M.HashMap FilePath LogFile -> IO LogFile
getOrCreateCurrent dir logs
  | M.null logs = createNewLog dir
  | otherwise = return $! head (M.elems logs)

close :: Hitcask -> IO ()
close h = do
  logs <- readTVarIO $ files h
  mapM_ (hClose . handle) $ M.elems logs

