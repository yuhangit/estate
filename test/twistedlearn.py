from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import task

def schedule_install(customer):
    def schedule_install_wordpress():
        def on_done():
            print("callabck: finshed installation for", customer)
        print("scheduling instalation for", customer)
        return  task.deferLater(reactor,3,on_done)
    def all_done():
        print("all done for", customer)

    d = schedule_install_wordpress()
    d.addCallback(all_done)
    return d

def twisted_developer_day(customers):
    print("Good morning from Twisted developer")
    work = [schedule_install(customer) for customer in customers]
    join = defer.DeferredList(work)
    join.addCallback(lambda _: reactor.stop())
    print("Bye from Twisted developer!")

@defer.inlineCallbacks
def inline_install(customer):
    print("Scheduling: installation for ", customer)
    yield task.deferLater(reactor,3 , lambda :None)
    print("callback: finished install for: ", customer)
    print("all done for ",customer)

def twisted_developer_days(customers):
    print("Good morning from Twisted developer")
    work = [inline_install(customer) for customer in customers]
    coop = task.Cooperator()
    join = defer.DeferredList([coop.coiterate(work) for i in range(5)])
    join.addCallback(lambda _: reactor.stop())
    print("Bye from Twisted developer!")

def main():
    twisted_developer_days(["customer %d" %i for i in range(15)])
    reactor.run()

if  __name__ == '__main__':
    main()