{-# LANGUAGE OverloadedStrings #-}
import Test.Hspec.Monadic
import Test.Hspec.HUnit()
import Test.HUnit
import Data.ByteString(ByteString)
import qualified Data.ByteString.Char8()
import qualified Data.HashMap.Strict as M
import Control.Concurrent.STM

data Hitcask = Hitcask (TVar (M.HashMap ByteString ByteString))

connect :: ByteString -> IO Hitcask
connect _ = do
  t <- newTVarIO M.empty
  return $! Hitcask t

get :: Hitcask -> ByteString -> IO (Maybe ByteString)
get (Hitcask t) key = do
  m <- readTVarIO t
  return $! M.lookup key m

put :: Hitcask -> ByteString -> ByteString -> IO Hitcask
put h@(Hitcask t) key value = atomically $ do
  modifyTVar' t $ \m ->
    M.insert key value m
  return $! h


main :: IO ()
main = hspec $
  describe "hitcask" $
    it "returns the value set as the key" $ do
      db <- connect "/tmp/hitcask/db01"
      put db "key" "value"
      (Just v) <- get db "key"
      v @?= "value"

