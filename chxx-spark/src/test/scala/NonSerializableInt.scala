class NonSerializableInt(val value: Int) {
  def +(that: NonSerializableInt) = new NonSerializableInt(value + that.value)
  def toInt() = value
}
