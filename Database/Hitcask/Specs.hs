{-# LANGUAGE OverloadedStrings #-}
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import System.Directory
import Database.Hitcask
import Database.Hitcask.Types

createEmpty :: FilePath -> IO Hitcask
createEmpty dir = do
  removeDirectoryRecursive dir
  connect dir

main :: IO ()
main = hspec $
  describe "hitcask" $ do
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

