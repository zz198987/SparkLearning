package com.bi.spark

trait BaseT {
  def getFilePath( name :String):String = getClass.getResource("/").getPath.concat("/").concat(name)
}
