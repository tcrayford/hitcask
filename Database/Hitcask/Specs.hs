{-# LANGUAGE OverloadedStrings #-}
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Test.Hspec.QuickCheck(prop)
import Control.Concurrent.STM
import Database.Hitcask
import Database.Hitcask.Types
import Database.Hitcask.Get
import Database.Hitcask.Restore
import Database.Hitcask.Logs
import Database.Hitcask.SpecHelper
import Database.Hitcask.Specs.Compact

main :: IO ()
main = hspec $ do
  openingLogFileSpecs
  compactSpecs
  describe "hitcask" $ do
    describe "get and put" $ do
      it "returns the value set as the key" $ do
        db <- createEmpty "/tmp/hitcask/db01"
        put db "key" "value"
        (Just v) <- get db "key"
        close db
        v @?= "value"

      it "persists the value after a restart" $ do
        db <- createEmpty "/tmp/hitcask/db02"
        put db "key" "value"
        close db
        db2 <- connect "/tmp/hitcask/db02"
        (Just v) <- get db2 "key"
        close db2
        v @?= "value"

      it "returns nothing if there is no value there" $ do
        db <- createEmpty "/tmp/hitcask/db03"
        n <- get db "key"
        close db
        n @?= Nothing

      it "persists and restores multiple keys" $ do
        db <- createEmpty "/tmp/hitcask/db04"
        put db "key" "value"
        put db "key2" "value2"
        close db
        db2 <- connect "/tmp/hitcask/db04"
        (Just v) <- get db2 "key"
        (Just v2) <- get db2 "key2"
        close db2
        v @?= "value"
        v2 @?= "value2"


    describe "deleting keys" $ do
      it "returns nothing if the key is deleted" $ do
        db <- createEmpty "/tmp/hitcask/db05"
        put db "key" "value"
        delete db "key"
        n <- get db "key"
        close db
        n @?= Nothing

      it "deletion persists accross sessions" $ do
        db <- createEmpty "/tmp/hitcask/db06"
        put db "key" "value"
        delete db "key"
        close db
        db2 <- connect "/tmp/hitcask/db06"
        n <- get db2 "key"
        close db
        n @?= Nothing

    describe "compacting" $
      it "reads the same key after compaction" $ do
        db <- createEmpty "/tmp/hitcask/db11"
        put db "key" "value"
        compact db
        (Just v) <- get db "key"
        close db
        v @?= "value"

readAllVals :: Hitcask -> [(Key, ValueLocation)] -> IO [Maybe Value]
readAllVals db = mapM (\(k,v) -> readFromLocation db v k) . reverse

openingLogFileSpecs :: Spec
openingLogFileSpecs = describe "getTimestamp" $ do
  it "gets the timestamp from a hitcask filename" $
    12345678 @?= getTimestamp "12345678.hitcask.data"

  it "the largest timestamp comes first" $
    head (byRecent ["12345.hitcask.data", "0.hitcask.data"]) @?= "12345.hitcask.data"


-- merging
-- key listing
-- folding
-- forcing writes to disk
