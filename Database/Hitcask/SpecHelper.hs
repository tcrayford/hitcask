module Database.Hitcask.SpecHelper where
import Database.Hitcask
import Database.Hitcask.Types
import Control.Monad
import System.Directory

createEmpty :: FilePath -> IO Hitcask
createEmpty dir = do
  whenM (doesDirectoryExist dir)
    (removeDirectoryRecursive dir)
  connect dir

whenM :: (Monad m) => m Bool -> m () -> m ()
whenM t a = t >>= flip when a

nTimes :: Int -> IO () -> IO ()
nTimes 0 a = a
nTimes n a = do
  a
  nTimes (n - 1) a

closeDB :: Hitcask -> IO ()
closeDB db = do
  close db
  removeDirectoryRecursive (dirPath db)

