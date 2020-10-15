#time "on"
#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
    
open System.Collections.Generic

type Gossip =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | StartGossip of String
    | ReportMsgRecvd of String
    | StartPushSum of Double
    | ComputePushSum of Double * Double * Double
    | Result of Double * Double
    | Time of int
    | TotalNodes of int
    | Active 
    | Call
    | InIt
type Topology = 
    | Gossip of String
    | PushSum of String

type Protocol = 
    | Line of String
    | Full of String
    | TwoDimension of String



//Point of execution
let mutable nodes =
    int (string (fsi.CommandLineArgs.GetValue 1))

let topology = string (fsi.CommandLineArgs.GetValue 2)
let protocol = string (fsi.CommandLineArgs.GetValue 3)


let timer = Diagnostics.Stopwatch()
let system = ActorSystem.Create("System")

let mutable actualNumOfNodes = nodes |> float

nodes <-
    match topology with
    | "2D" | "Imp2D" -> 
        ((actualNumOfNodes ** 0.5)|>ceil ) ** 2.0 |> int
    | _ -> nodes


let mutable  nodeArray = [||]

let Supervisor(mailbox: Actor<_>) =
    
    let mutable count = 0
    let mutable start = 0
    let mutable totalNodes = 0

    let rec loop()= actor{
        let! msg = mailbox.Receive();
        match msg with 
        | ReportMsgRecvd _ -> 
            let ending = DateTime.Now.TimeOfDay.Milliseconds
            count <- count + 1

            //printfn "converged %d" count
            if count = totalNodes then
                timer.Stop()
                printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)

        | Result (sum, weight) ->
            timer.Stop()
            printfn "Sum = %f Weight= %f Average=%f" sum weight (sum / weight)
            printfn "Time for convergence: %f ms" timer.Elapsed.TotalMilliseconds
            Environment.Exit(0)
        | Time strtTime -> start <- strtTime

        | TotalNodes n -> totalNodes <- n
        | _ -> ()

        return! loop()
    }            
    loop()




let supervisor = spawn system "Supervisor" Supervisor
let dictionary = new Dictionary<IActorRef, bool>()


let Worker(mailbox: Actor<_>) =
    let mutable rumourCount = 0
    let mutable neighbours: IActorRef [] = [||]
    let mutable sum = 0 |>float
    let mutable weight = 1.0
    let mutable termRound = 1
    
    
    let rec loop()= actor{
        let! message = mailbox.Receive();
        
        match message with 

        | Initailize aref -> neighbours <- aref

        | Active ->
            if rumourCount < 11 then
                let rnd = Random().Next(0, neighbours.Length)
                if not dictionary.[neighbours.[rnd]] then
                    neighbours.[rnd] <! Call
                mailbox.Self <! Active

        | Call ->
            
            if rumourCount = 0 then 
                mailbox.Self <! Active
            if (rumourCount = 10) then 
                supervisor <! ReportMsgRecvd "Rumor"
                dictionary.[mailbox.Self] <- true
            rumourCount <- rumourCount + 1
            
        | InitializeVariables number ->
            sum <- number |> double


        | StartPushSum delta ->
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! ComputePushSum(sum, weight, delta)

        | ComputePushSum (s: float, w, delta) ->
            let newsum = sum + s
            let newweight = weight + w

            let cal =
                sum / weight - newsum / newweight |> abs


            match (s, w, delta) with
            | (_, _, delta) when cal > delta -> 
                termRound <- 0
                sum <- sum + s
                weight <- weight + w
                sum <- sum / 2.0
                weight <- weight / 2.0

                let index =
                    Random().Next(0, neighbours.Length)

                neighbours.[index]
                <! ComputePushSum(sum, weight, delta)

            | (_, _, _) when termRound >= 3 -> 
                supervisor <! Result(sum, weight)
            | _ -> 
                sum <- sum / 2.0
                weight <- weight / 2.0
                termRound <- termRound + 1

                let index =
                    Random().Next(0, neighbours.Length)

                neighbours.[index]
                <! ComputePushSum(sum, weight, delta)
        | _ -> ()

        return! loop()
    }            
    loop()



let gossipConvergentActor (mailbox: Actor<_>) =
    let neighbors = new List<IActorRef>()

    let rec loop() = actor {
        let! message = mailbox.Receive()
        match message with 
        | InIt _ ->
            for i in [0..nodes-1] do
                    neighbors.Add nodeArray.[i]
            mailbox.Self <! Active
        | Active ->
            if neighbors.Count > 0 then
                let randomNum = Random().Next(neighbors.Count)
                let randomActor: IActorRef = neighbors.[randomNum]

                // Check random actor's status
                let converged: bool = dictionary.[randomActor]

                // while random actor is already converged, pick next neighbor
                if (converged) then  
                    // Removing converged actor from current actor's neighbors list
                    neighbors.Remove randomActor
                else 
                    randomActor <! Call

                mailbox.Self <! Active 

        return! loop()
    }
    loop()

let GossipActor = spawn system "GossipConvergentActor" gossipConvergentActor

match topology with
| "line" ->
    nodeArray <- Array.zeroCreate (nodes + 1)
    

    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) Worker
        nodeArray.[x] <- actorRef 
        dictionary.Add(nodeArray.[x], false)
        nodeArray.[x] <! InitializeVariables x

    

    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        if i = 0 then
            neighbourArray <- (Array.append neighbourArray [|nodeArray.[i+1]|])
        elif i = nodes then
            neighbourArray <- (Array.append neighbourArray [|nodeArray.[i-1]|])
        else 
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[(i - 1)] ; nodeArray.[(i + 1 ) ] |] ) 
        
        nodeArray.[i] <! Initailize(neighbourArray)

    
    let leader = Random().Next(0, nodes)

    timer.Start()
    match protocol with
    | "gossip" -> 
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Protocol Gossip"
        nodeArray.[leader] <! Active
        GossipActor<! InIt
        
    | "push-sum" -> 
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum Protocol for Line"
        nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)     
    | _ ->
        printfn "Invlaid topology"   

| "full" ->
    nodeArray <- Array.zeroCreate (nodes + 1)
   
    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) Worker
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
    [0..nodes] |> List.iter (fun i -> nodeArray.[i] <! Initailize(nodeArray))
    timer.Start()
    let leader = Random().Next(0, nodes)

    match protocol with
    | "gossip" -> 
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Gossip -------------"
        nodeArray.[leader] <! Call
    | "push-sum" -> 
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Push Sum -------------"
        nodeArray.[leader] <! StartPushSum(10.0 ** -10.0) 
    | _ ->
        printfn "Invalid topology"

| "2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int 

    
    nodeArray <- Array.zeroCreate (nodes+1)
    
    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) Worker
        
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
        
    

    for i in [ 0 .. (gridSize-1)] do
        for j in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if j + 1 < gridSize then
                //printfn "right %d %d formula %d" i j ((i * gridSize) + j + 1)
                neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j + 1] |])
            if  j - 1 >= 0 then 
                //printfn "left %d %d formula %d" i j ((i * gridSize) + j - 1)
                neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j - 1] |])
            if i - 1 >= 0 then
                //printfn "top %d %d  formula %d" i j ((i - 1 ) * gridSize + j)
                neighbours <- Array.append neighbours [| nodeArray.[(i - 1 ) * gridSize + j ] |]
            if  i + 1 < gridSize then
                //printfn "bottom %d %d  formula %d %d grid size " i j ((i + 1 ) * gridSize + j) gridSize
                neighbours <- (Array.append neighbours [| nodeArray.[(i + 1) * gridSize + j] |])
            nodeArray.[i * gridSize + j] <! Initailize(neighbours)
   
    
    let leader = Random().Next(0, nodes)
    timer.Start()
    match protocol with 
    | "gossip" -> 
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Gossip -------------"
        nodeArray.[leader] <! Active
        GossipActor<! InIt
    | "push-sum" -> 
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Push Sum -------------"
        nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)
    | _ -> 
        printfn "Invalid topology"

| "Imp2D" ->
    let gridSize = nodes |> float |> sqrt |> ceil |> int 

    
    nodeArray <- Array.zeroCreate (nodes+1)
    
    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) Worker
        
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
        
    //printfn "if"

    for i in [ 0 .. (gridSize-1)] do
        for j in [ 0 .. (gridSize-1) ] do
            let mutable neighbours: IActorRef [] = [||]
            if j + 1 < gridSize then
                //printfn "right %d %d formula %d" i j ((i * gridSize) + j + 1)
                neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j + 1] |])
            if  j - 1 >= 0 then 
                //printfn "left %d %d formula %d" i j ((i * gridSize) + j - 1)
                neighbours <- (Array.append neighbours [| nodeArray.[i * gridSize + j - 1] |])
            if i - 1 >= 0 then
                //printfn "top %d %d  formula %d" i j ((i - 1 ) * gridSize + j)
                neighbours <- Array.append neighbours [| nodeArray.[(i - 1 ) * gridSize + j ] |]
            if  i + 1 < gridSize then
                //printfn "bottom %d %d  formula %d %d grid size " i j ((i + 1 ) * gridSize + j) gridSize
                neighbours <- (Array.append neighbours [| nodeArray.[(i + 1) * gridSize + j] |])

            let rnd = Random().Next(0, nodes-1)
            neighbours <- (Array.append neighbours [|nodeArray.[rnd] |])
            nodeArray.[i * gridSize + j] <! Initailize(neighbours)
   
    printf "leader"
    let leader = Random().Next(0, nodes)
    timer.Start()
    match protocol with 
    | "gossip" -> 
        supervisor <! TotalNodes(nodes)
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Gossip -------------"
        nodeArray.[leader] <! Active
        GossipActor<! InIt
    | "push-sum" -> 
        supervisor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "------------- Begin Push Sum -------------"
        nodeArray.[leader] <! StartPushSum(10.0 ** -10.0)
    | _ -> 
        printfn "Invalid topology"
| _ -> ()

Console.ReadLine() |> ignore
    
