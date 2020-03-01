package com.svend.demo

object DataModel {

  case class PizzaId(id: Int)
  case class Person(name: String, firstName: String)
  case class Pizza(name: String, vegetarian: Boolean, vegan: Boolean, calories: Int, chef: Person)

}
