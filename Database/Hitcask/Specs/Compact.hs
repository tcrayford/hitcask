{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.Specs.Compact(compactSpecs) where
import Test.Hspec.Monadic
import Test.Hspec.QuickCheck
import Test.Hspec.HUnit()
import Test.HUnit hiding(path)
import Data.Serialize.Get
import Control.Concurrent.STM
import Database.Hitcask.Types
import Database.Hitcask.Compact
import Database.Hitcask.SpecHelper
import Database.Hitcask.Rotation
import Database.Hitcask.Restore
import Database.Hitcask.Put
import Database.Hitcask.Hint
import Database.Hitcask.Logs
import Database.Hitcask.Specs.Arbitrary
import Database.Hitcask
import qualified Data.HashMap.Strict as M
import System.IO(stdin)

compactSpecs :: Spec
compactSpecs = do
  allNonActiveSpecs
  compactLogFileSpecs
  replaceNonActiveSpecs
  addMergedKeyDirSpecs
  hintFileSpecs
  addMergedLogSpecs
  parsingRoundTripSpecs


allNonActiveSpecs :: Spec
allNonActiveSpecs = describe "allNonActive" $ do
  it "doesn't contain the active log file" $ do
    db <- createEmpty "/tmp/hitcask/db08"
    a <- fmap current $ readTVarIO $ logs db
    old <- allNonActive db
    elem a old @?= False

  it "contains another log file" $ do
    db <- createEmpty "/tmp/hitcask/db09"
    a <- fmap current $ readTVarIO $ logs db
    rotateLogFile db
    old <- allNonActive db
    elem a old @?= True

  it "doesn't contain merged log files" $
    nonActive (HitcaskLogs (LogFile stdin "current") (M.fromList [("/tmp/foo/1234.hitcask.data.merged", LogFile stdin "/tmp/foo/1234.hitcask.data.merged")])) @?= []


compactLogFileSpecs :: Spec
compactLogFileSpecs = describe "compactLogFile" $
  it "produces a new log file with no duplicate keys" $ do
    l <- openLogFile "/tmp/hitcask/db09/logfile.test"
    _ <- writeValue l "key" "value"
    _ <- writeValue l "key" "value2"
    (l2, _) <- compactLogFile l
    k <- allKeys (mergedLog l2)
    map fst k @?= ["key"]

replaceNonActiveSpecs :: Spec
replaceNonActiveSpecs = describe "replaceNonActive" $
  it "puts the updated log file in the db" $ do
    db <- createEmpty "/tmp/hitcask/db10"
    c <- readTVarIO $ logs db
    rotateLogFile db
    l <- createMergedLog (current c)
    replaceNonActive db [(l, M.empty)]
    old <- fmap files . readTVarIO $ logs db
    Just (mergedLog l) @?= M.lookup (path $ mergedLog l) old

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
    c <- readTVarIO $ logs db
    rotateLogFile db
    m <- createMergedLog (current c)
    _ <- appendToLog' m ("key", (10000000000000, "value"))
    original <- restoreFromLog (current c)
    close db
    restored <- loadHintFile (hintFile m)
    restored @?= original

addMergedLogSpecs :: Spec
addMergedLogSpecs = describe "addMergedLog" $ do
  it "removes the old log" $ do
    let newLog = MergingLog (LogFile stdin "merged") "logfile" (LogFile stdin "hint")
        ls     = HitcaskLogs (LogFile stdin "current") (M.fromList [("logfile", LogFile stdin "logfile")])
    Nothing @?= M.lookup "logfile" (files (removeMergedLog newLog ls))

  it "adds a new log" $ do
    let newLog = MergingLog (LogFile stdin "merged") "logfile" (LogFile stdin "hint")
        ls     = HitcaskLogs (LogFile stdin "current") (M.fromList [("logfile", LogFile stdin "logfile")])
    Just (LogFile stdin "merged") @?= M.lookup "merged" (files (addNewLog newLog ls))



parsingRoundTripSpecs :: Spec
parsingRoundTripSpecs = describe "parsing a hint" $
  prop "restores the same value location" propParsingRoundTrip

propParsingRoundTrip :: NonEmptyKey -> ValueLocation -> Bool
propParsingRoundTrip (NonEmptyKey k) loc = restored == (k, loc)
  where (Right restored) = runGet readLoc (hint k loc)

