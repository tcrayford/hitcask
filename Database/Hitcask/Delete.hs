{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Delete where
import Database.Hitcask.Types
import Database.Hitcask.Put
import Data.ByteString.Char8()

delete :: Hitcask -> Key -> IO Hitcask
delete h key = put h key "<<hitcask_tombstone>>"
