{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Delete where
import Database.Hitcask.Types
import Database.Hitcask.Put
import Data.ByteString.Char8()
import qualified Data.HashMap.Strict as M
import Control.Concurrent.STM

delete :: Hitcask -> Key -> IO ()
delete h key = do
  put h key "<<hitcask_tombstone>>"
  atomically $ modifyTVar (keys h) (M.delete key)

