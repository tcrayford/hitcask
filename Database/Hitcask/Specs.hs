{-# LANGUAGE OverloadedStrings #-}
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import System.Directory
import Database.Hitcask
import Control.Monad(when)

whenM :: (Monad m) => m Bool -> m () -> m ()
whenM t a = t >>= flip when a

createEmpty :: FilePath -> IO Hitcask
createEmpty dir = do
  whenM (doesDirectoryExist dir)
    (removeDirectoryRecursive dir)
  connect dir

main :: IO ()
main = hspec $
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

