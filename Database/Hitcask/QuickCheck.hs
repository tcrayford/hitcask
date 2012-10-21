{-# LANGUAGE OverloadedStrings #-}
module Database.Hitcask.QuickCheck where
import Database.Hitcask
import Database.Hitcask.Types
import Database.Hitcask.SpecHelper
import Test.QuickCheck
import Test.QuickCheck.Monadic
import qualified Data.ByteString as B

prop_get_retreives_what_was_put :: Key -> Value -> Property
prop_get_retreives_what_was_put k v = monadicIO $ do
  h <- run $ createEmpty "/tmp/hitcask/db011"
  run $ put h k v
  (Just readV) <- run $ get h k
  assert $ readV == v
  run $ close h

instance Arbitrary B.ByteString where
    arbitrary   = fmap B.pack arbitrary

