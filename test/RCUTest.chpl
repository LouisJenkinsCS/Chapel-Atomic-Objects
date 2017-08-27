use RCU;
use Time;

class C {
	var x = 1;
}

var rcu : RCU(C);

begin {
	while true {
		rcu.readBarrier(lambda(c : C) {
			if c == nil {
				writeln("Read: (NULL)");
			}

			writeln("Read: ", c.x);
		});
		sleep(1);
	}
}

while true {
	rcu.writeBarrier(lambda(c : C) : C { 
		if c == nil {
			return new C(0);
		}

		return new C(c.x + 1); 
	});
	sleep(2);
}

