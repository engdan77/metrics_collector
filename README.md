# Metrics Collector



## Motivation and background

Now I've recently become more and more fascinated by good software design by following design principles such as SOLID. And reading and learn the different design patterns for solving problems have got myself challenge myself thinking harder what might be found applicable for building a easily extensible and maintainable Python software project .. so what project might that be ..?

Thankfully (but not unusual) I did come across a daily problem of mine but I imagine being something others might also have good reasons to solve, and at the time of writing this I had not yet come across any other project to address those.

So this is when the "**Metric Collector**" was first invented, which is an application to be highly customisable for collecting metric from different data points, deal with the processing and expose certain basic user interfaces such as RESTful API, user-friendly Web interface and also a scheduler to allow e.g. sending these time-based graphs.

And the code been written in such way, and thankfully to Pythons dynamic nature automatically make this into a more seamless experience thanks to the only work needed to extending additional services is to inherit few different base-classes.

This also gave me a chance to familiarize myself with GitHub actions allow automatic the CI/CD pipeline into the PyPI.

....

## Installation

....

## Usage

....

## Software design

So the main idea behind is primarily to abstract the logic that chenges the most and with a few different abstract classes supposed to represent each phase most commonly referred as Extract -> Transform -> Load .. so in short summary you would need to implement the following abstract classes

BaseExtract
BaseTransform
BaseLoad

And implement their abstract methods which with help from code-completion of your IDE will make it relatively easy.
On startup of this application we'll use the class inheritance and and their "dag_name" as being the mutual key that binds these steps into what I might be abuse the term related to DAG to expose them as option usable from the different user interfaces.

....

Facade pattern (Orchestrator)
Strategy pattern (passing progress bar per reference)
Template method pattern



## Credits

.....