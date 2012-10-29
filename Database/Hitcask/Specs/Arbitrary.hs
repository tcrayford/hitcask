{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Hitcask.Specs.Arbitrary where
import Database.Hitcask.Types
import Test.QuickCheck
import qualified Data.ByteString as B

instance Arbitrary B.ByteString where
  arbitrary = fmap B.pack arbitrary

instance Arbitrary ValueLocation where
  arbitrary = do
    f <- arbitrary
    vs <- arbitrary
    vp <- arbitrary
    ts <- arbitrary
    return $! ValueLocation f vs vp ts

newtype HitcaskFilePath = HitcaskFilePath FilePath

instance Arbitrary HitcaskFilePath where
  arbitrary = elements $ map (HitcaskFilePath . ("/tmp/hitcask/arbitrarydb" ++) . show) ([0..10] :: [Integer])


newtype NonEmptyKey = NonEmptyKey Key deriving (Show)

instance Arbitrary NonEmptyKey where
  arbitrary = fmap (NonEmptyKey . B.append "key_") arbitrary


