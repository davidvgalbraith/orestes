fields.collect{
  def val = doc[it].value
  if (val == null) {
    return "0z";
  }
  return (val.class == String) ? (val.length() + "s" + val) : (val + "n");
}.join("")
