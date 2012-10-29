{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Hitcask.Specs.Arbitrary where
import Database.Hitcask.Types
import Test.QuickCheck
import qualified Data.ByteString as B
import GHC.Word(Word8)

instance Arbitrary B.ByteString where
  arbitrary = fmap B.pack arbitrary

newtype LowerCase = LowerCase String

instance Arbitrary LowerCase where
  arbitrary = elements $ map (LowerCase . ("key_" ++ ) . (:[])) ['a'..'z']

instance Arbitrary ValueLocation where
  arbitrary = do
    (LowerCase f) <- arbitrary
    (Positive vs) <- arbitrary
    (Positive vp) <- arbitrary
    (Positive ts) <- arbitrary
    return $! ValueLocation f vs vp ts

newtype HitcaskFilePath = HitcaskFilePath FilePath

instance Arbitrary HitcaskFilePath where
  arbitrary = elements $ map (HitcaskFilePath . ("/tmp/hitcask/arbitrarydb" ++) . show) ([0..10] :: [Integer])


newtype NonEmptyKey = NonEmptyKey Key deriving (Show)

instance Arbitrary NonEmptyKey where
  arbitrary = do
    (LowerCase a) <- arbitrary
    return $! NonEmptyKey $ B.pack (map c2w8 a)

c2w8 ::  Char -> Word8
c2w8 = fromIntegral . fromEnum

