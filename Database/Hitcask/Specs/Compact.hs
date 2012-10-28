{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Specs.Compact(compactSpecs) where
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Control.Concurrent.STM
import Database.Hitcask.Types
import Database.Hitcask.Compact
import Database.Hitcask.SpecHelper
import Database.Hitcask.Rotation
import Database.Hitcask.Put
import Database.Hitcask.Hint
import Database.Hitcask.Logs
import Database.Hitcask
import Database.Hitcask.Restore
import qualified Data.HashMap.Strict as M

compactSpecs :: Spec
compactSpecs = do
  allNonActiveSpecs
  compactLogFileSpecs
  replaceNonActiveSpecs
  addMergedKeyDirSpecs
  hintFileSpecs

allNonActiveSpecs :: Spec
allNonActiveSpecs = describe "allNonActive" $ do
  it "doesn't contain the active log file" $ do
    db <- createEmpty "/tmp/hitcask/db08"
    a <- readTVarIO $ current db
    nonActive <- allNonActive db
    elem a nonActive @?= False

  it "contains another log file" $ do
    db <- createEmpty "/tmp/hitcask/db09"
    a <- readTVarIO $ current db
    rotateLogFile db
    nonActive <- allNonActive db
    elem a nonActive @?= True

compactLogFileSpecs :: Spec
compactLogFileSpecs = describe "compactLogFile" $
  it "produces a new log file with no duplicate keys" $ do
    l <- openLogFile "/tmp/hitcask/db09/logfile.test"
    writeValue l "key" "value"
    writeValue l "key" "value2"
    (l2, _) <- compactLogFile l
    k <- allKeys (mergedLog l2)
    map fst k @?= ["key"]

replaceNonActiveSpecs :: Spec
replaceNonActiveSpecs = describe "replaceNonActive" $
  it "puts the updated log file in the db" $ do
    db <- createEmpty "/tmp/hitcask/db10"
    c <- readTVarIO $ current db
    rotateLogFile db
    l <- createMergedLog c
    replaceNonActive db [(l, M.empty)]
    nonActive <- allNonActive db
    head nonActive @?= mergedLog l

addMergedKeyDirSpecs :: Spec
addMergedKeyDirSpecs = describe "latestWrite" $
  it "picks the latest write out of two value locations" $ do
    let base = ValueLocation "path" 10 10 10
        earlier = base { timestamp = 1 }
        later = base { timestamp = 100000000 }
    latestWrite earlier later @?= later

hintFileSpecs :: Spec
hintFileSpecs = describe "writing and restoring from hint files" $
  it "restores the same keydir as read from the log" $ do
    db <- createEmpty "/tmp/hitcask/db13"
    c <- readTVarIO $ current db
    rotateLogFile db
    m <- createMergedLog c
    appendToLog' m ("key", "value")
    original <- restoreFromFile c
    close db
    restored <- restoreFromHintFile (hintFile m)
    restored @?= original
