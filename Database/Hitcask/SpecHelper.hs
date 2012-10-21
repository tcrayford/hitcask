module Database.Hitcask.SpecHelper where
import Database.Hitcask
import Control.Monad
import System.Directory

createEmpty :: FilePath -> IO Hitcask
createEmpty dir = do
  whenM (doesDirectoryExist dir)
    (removeDirectoryRecursive dir)
  connect dir

whenM :: (Monad m) => m Bool -> m () -> m ()
whenM t a = t >>= flip when a
