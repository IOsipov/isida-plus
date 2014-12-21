  var DiaStr = " \n\
isida.py->kernel.py: Manage threads \n\
kernel.py-->plugins/\*.py: Load plugins \n\
Note right of plugins/\*.py:Inherited from\\ntemplate class \n\
plugins/\*.py-->kernel.py: Announces\\nsupported commands \n\
Note left of kernel.py: Receive XMPP message \n\
kernel.py->plugins/\*.py: Send data \n\
Note right of plugins/\*.py: Process data \n\
plugins/\*.py->kernel.py: Send response \n\
Note left of kernel.py: Send XMPP message";
  var diagram = Diagram.parse(DiaStr);
  diagram.drawSVG("bot-kernel");
