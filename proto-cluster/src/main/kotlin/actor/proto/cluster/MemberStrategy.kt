package actor.proto.cluster

/**
 * MemberStrategy is responsible for selecting a member to host an actor.
 */
interface MemberStrategy {
    /**
     * Add a member to the strategy.
     * @param memberId The ID of the member to add.
     */
    fun addMember(memberId: String)
    
    /**
     * Remove a member from the strategy.
     * @param memberId The ID of the member to remove.
     */
    fun removeMember(memberId: String)
    
    /**
     * Get the partition member for a given identity.
     * @param identity The identity of the actor.
     * @return The ID of the member that should host the actor, or null if no members are available.
     */
    fun getPartition(identity: String): String?
}

/**
 * RoundRobinMemberStrategy selects members in a round-robin fashion.
 */
class RoundRobinMemberStrategy : MemberStrategy {
    private val members = mutableListOf<String>()
    private var index = 0
    
    override fun addMember(memberId: String) {
        synchronized(members) {
            if (!members.contains(memberId)) {
                members.add(memberId)
            }
        }
    }
    
    override fun removeMember(memberId: String) {
        synchronized(members) {
            members.remove(memberId)
            if (index >= members.size && members.isNotEmpty()) {
                index = 0
            }
        }
    }
    
    override fun getPartition(identity: String): String? {
        synchronized(members) {
            if (members.isEmpty()) {
                return null
            }
            
            val member = members[index]
            index = (index + 1) % members.size
            return member
        }
    }
}

/**
 * RendezvousMemberStrategy selects members using the rendezvous hashing algorithm.
 */
class RendezvousMemberStrategy : MemberStrategy {
    private val members = mutableSetOf<String>()
    
    override fun addMember(memberId: String) {
        synchronized(members) {
            members.add(memberId)
        }
    }
    
    override fun removeMember(memberId: String) {
        synchronized(members) {
            members.remove(memberId)
        }
    }
    
    override fun getPartition(identity: String): String? {
        synchronized(members) {
            if (members.isEmpty()) {
                return null
            }
            
            var maxScore = Double.NEGATIVE_INFINITY
            var maxMember: String? = null
            
            for (member in members) {
                val score = score(identity, member)
                if (score > maxScore) {
                    maxScore = score
                    maxMember = member
                }
            }
            
            return maxMember
        }
    }
    
    /**
     * Calculate the score for a given identity and member.
     * @param identity The identity of the actor.
     * @param member The ID of the member.
     * @return The score.
     */
    private fun score(identity: String, member: String): Double {
        val hash = (identity + member).hashCode()
        return hash.toDouble() / Int.MAX_VALUE
    }
}
