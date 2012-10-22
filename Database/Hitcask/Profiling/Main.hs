{-# LANGUAGE OverloadedStrings #-}
import Database.Hitcask.SpecHelper
import Database.Hitcask

profiledGet :: IO ()
profiledGet = do
  h <- createEmpty "/tmp/hitcask/profiledGet"
  put h "key" "value"
  nTimes 1000 $ do
    get h "key"
    return ()
  return ()

main :: IO ()
main = profiledGet

