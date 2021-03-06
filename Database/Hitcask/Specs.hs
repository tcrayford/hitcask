{-# LANGUAGE OverloadedStrings #-}
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Database.Hitcask
import Database.Hitcask.Rotation
import Database.Hitcask.Types
import Database.Hitcask.Get
import Database.Hitcask.Logs
import Database.Hitcask.SpecHelper
import Database.Hitcask.Specs.Compact
import Database.Hitcask.Specs.QuickCheck
import Test.Hspec.QuickCheck


main :: IO ()
main = hspec $ do
  openingLogFileSpecs
  compactSpecs
  describe "quickCheck" $
    prop "it passes quickCheck postconditions" propCheckPostConditions
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

    describe "compacting" $ do
      it "reads the same key after compaction" $ do
        db <- createEmptyWith "/tmp/hitcask/db11" (standardSettings { maxBytes = 1 })
        put db "key" "v1"
        put db "key" "v2"
        compact db
        v <- get db "key"
        close db
        v @?= Just "v2"

      it "doesn't lock files upon compaction and closing" $ do
        db <- createEmpty "/tmp/hitcask/db13"
        put db "key" "value"
        rotateLogFile db
        compact db
        close db
        db2 <- connect "/tmp/hitcask/db13"
        v <- get db2 "key"
        close db2
        v @?= Just "value"

    describe "listing keys" $
      it "lists the available keys" $ do
        db <- createEmpty "/tmp/hitcask/db12"
        put db "key" "value"
        ks <- listKeys db
        close db
        ks @?= ["key"]

readAllVals :: Hitcask -> [(Key, ValueLocation)] -> IO [Maybe Value]
readAllVals db = mapM (\(k,v) -> readFromLocation db v k) . reverse

openingLogFileSpecs :: Spec
openingLogFileSpecs = describe "logs" $ do
  describe "getTimestamp" $ do
    it "gets the timestamp from a hitcask filename" $
      12345678 @?= getTimestamp "12345678.hitcask.data"

    it "the largest timestamp comes first" $
      head (byRecent ["12345.hitcask.data", "0.hitcask.data"]) @?= "12345.hitcask.data"

  describe "isMerged" $ do
    it "is merged if the file ends with .merged" $
      isMerged "/tmp/foo/1234123.hitcask.data.merged" @?= True

    it "normal log files are not merged" $
      isMerged "/tmp/foo/1234123.hitcask.data" @?= False


-- an bug: merging doesn't always work
-- duplication between reading log files?
-- producing hint files
-- using hint files on startup
-- folding
-- forcing writes to disk
