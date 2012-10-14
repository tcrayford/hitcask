{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Delete where
import Database.Hitcask.Types
import Database.Hitcask.Put
import Data.ByteString(ByteString)
import Data.ByteString.Char8()

delete :: Hitcask -> ByteString -> IO Hitcask
delete h key = put h key "<<hitcask_tombstone>>"
