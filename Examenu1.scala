//1//Verifique solo 1 nuemero par
def par(num:Int): Boolean =
  {
  if(num%2==0){
    true}
  else{
    false}
  }
par(8)

//2//Buscar  numeros pares en la lista
val numeros = List (3,4,5,7)
def check(num:List[Int]): Boolean ={ return (num(1)%2==0)}
println(check(numeros))


//3//fortunado numero 7.
def afor(list:List[Int]): Int={
  var res=0
  for(n <- list){
    if (n==7){
      res = res + 14
    } else {
      res = res + n
    }
  }
  return res
}
val numeros= List(5,4,7,7,3)
println(afor(numeros))

//4//Puedes equilibrar
val lista = List(8,3,2)
var a = lista.length
var b = (a%2)-1
var suma = 0
var x = b+1
for (x<-x to a){
  suma = suma + lista (x)
}
if (lista(b)==suma){
  (lista(b)==suma)
}else {
  (lista(b)==suma)
}

//5//verificar polindromo
def palindromo(palabra:String) :Boolean ={
return (palabra==palabra.reverse)
}
val palabra = "radar"
val palabra2 = "auto"

println(palindromo(palabra))
println(palindromo(palabra2))
