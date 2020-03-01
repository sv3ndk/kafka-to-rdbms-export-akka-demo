package com.svend.demo

object DataModel {

  // this is used as the kafka key
  case class PizzaId(id: Int)

  case class Person(name: String, firstName: String)

  // this is used as the kafka value
  case class Pizza(name: String, vegetarian: Boolean, vegan: Boolean, calories: Int, chef: Person)

}
