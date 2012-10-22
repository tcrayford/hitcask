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

profiledPut :: IO ()
profiledPut = do
  h <- createEmpty "/tmp/hitcask/profiledGet"
  nTimes 1000 $ do
    put h "key" "value"
    return ()
  return ()

main :: IO ()
main = profiledPut

